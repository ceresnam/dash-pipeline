package com.modrykonik.dash.io;

import java.util.ArrayList;

import org.joda.time.LocalDate;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.transforms.Partition.PartitionFn;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.DashPipeline;
import com.modrykonik.dash.model.UserStatsRow;

/**
 * Read and write UserStatsRow from sharded big query table
 */
public class BQUserStatsIO {

	private static final LocalDate DATA_FROM = new LocalDate(2010, 1, 1);

	/**
	 * Read UserStatsRow rows from sharded big query table for given date range.
	 */
	public static class Read
		extends PTransform<PInput, PCollection<TableRow>>
	{
		private final String[] bqTables;

		public Read(String[] bqTables) {
			this.bqTables = bqTables;
		}

		public static Read from(int serverId, LocalDate dfrom, LocalDate dto) {
			return new Read(tableNames(serverId, dfrom, dto));
		}

	    @Override
	    public PCollection<TableRow> apply(PInput input) {

	    	Pipeline pipe = input.getPipeline();

		    PCollectionList<TableRow> bqrows = PCollectionList.empty(pipe);
	        for (String inTable: bqTables) {
	        	String ym = inTable.substring(inTable.length()-7);
	        	PCollection<TableRow> bqTableRows = pipe.apply("BQRead-"+ym, BigQueryIO.Read.from(inTable));
	        	bqrows = bqrows.and(bqTableRows);
	        }

	        return bqrows.apply("BQConcat", Flatten.pCollections());
	    }

	    /**
	     * Returns list of table names that include date range dfrom (inclusive) - dto (inclusive)
	     */
		private static String[] tableNames(int serverId, LocalDate dfrom, LocalDate dto) {
			int yFrom = dfrom.isBefore(DATA_FROM) ? 2010 : dfrom.getYear();
			int mFrom = dfrom.isBefore(DATA_FROM) ? 1 : dfrom.getMonthOfYear();
			int yTo = dto.getYear();
			int mTo = dto.getMonthOfYear();

			ArrayList<String> bqTables = new ArrayList<String>();
			for (int y=yFrom; y<=yTo; y++) {
				for (int m=1; m<=12; m++) {
					if (y==yFrom && m<mFrom)
						continue;
					if (y==yTo && m>mTo)
						continue;

					String bqTable = String.format(
						"%s:user_stats_%d.ds_daily_user_stats_%d_%02d",
						DashPipeline.PROJECT_ID, serverId, y, m
					);
					bqTables.add(bqTable);
				}
			}

			return bqTables.toArray(new String[bqTables.size()]);
		}

	}

	/**
	 * Write rows of given date range to sharded big query table.
	 */
	public static class Write
		extends PTransform<PCollection<UserStatsRow>, PDone>
	{
		private final int serverId;
		private final LocalDate dfrom;
		private final LocalDate dto;

		protected Write(int serverId, LocalDate dfrom, LocalDate dto) {
			this.serverId = serverId;
			this.dfrom = dfrom;
			this.dto = dto;
		}

		public static Write to(int serverId, LocalDate dfrom, LocalDate dto) {
			return new Write(serverId, dfrom, dto);
		}

		@Override
		public PDone apply(PCollection<UserStatsRow> input) {

			//split urows into partitions, based on calendar month
			PCollectionList<UserStatsRow> tables = input
			    .apply("Partition", Partition.of(monthsBetween(dfrom, dto)+1, new PartitionFn<UserStatsRow>() {
			    	@Override
			    	public int partitionFor(UserStatsRow urow, int numPartitions) {
			    		assert urow.day.isAfter(dfrom) || urow.day.isEqual(dfrom);
			    		assert urow.day.isBefore(dto) || urow.day.isEqual(dto);
			    		return monthsBetween(dfrom, urow.day);
			    	}
			    }));

			//write each partition into own bq table
			for (int i=0; i<tables.size(); i++) {
				PCollection<UserStatsRow> table = tables.get(i);
				LocalDate tableFrom = dfrom.plusMonths(i);
				String outTable = tableShardName(tableFrom.getYear(), tableFrom.getMonthOfYear());
				String ym = outTable.substring(outTable.length()-16, outTable.length()-9);

				table
			        .apply("BQExport-"+ym, MapElements
						.via((UserStatsRow urow) -> urow.toBQTableRow())
	                	.withOutputType(new TypeDescriptor<TableRow>() {}))
					.apply("BQWrite-"+ym, BigQueryIO.Write
						.to(outTable)
						.withSchema(UserStatsRow.toBQTableSchema())
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
			}

			return PDone.in(input.getPipeline());
		}

		private String tableShardName(int year, int month) {
			String bqTable = String.format(
				"%s:user_stats_%d.ds_daily_user_stats_%d_%02d_computed",
				DashPipeline.PROJECT_ID, this.serverId, year, month
			);

			return bqTable;
		}

		private int monthsBetween(LocalDate d1, LocalDate d2) {
			return (d2.getYear() - d1.getYear()) * 12 + d2.getMonthOfYear() - d1.getMonthOfYear();
		}

    }

}