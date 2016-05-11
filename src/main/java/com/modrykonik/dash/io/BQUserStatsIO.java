package com.modrykonik.dash.io;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.DashPipeline;
import com.modrykonik.dash.model.UserStatsRow;
import org.joda.time.LocalDate;

import java.util.ArrayList;

/**
 * Read and write UserStatsRow from sharded big query table
 */
public class BQUserStatsIO {

	private static final LocalDate DATA_FROM = new LocalDate(2010, 1, 1);

	/**
	 * Read UserStatsRow rows from sharded big query table for given date range.
	 */
	public static class Read
		extends PTransform<PInput, PCollection<UserStatsRow>>
	{
		private final String[] bqTables;

		private Read(String[] bqTables) {
			this.bqTables = bqTables;
		}

		public static Read from(int serverId, LocalDate dfrom, LocalDate dto) {
			return new Read(tableNames(serverId, dfrom, dto));
		}

	    @Override
	    public PCollection<UserStatsRow> apply(PInput input) {
	    	Pipeline pipe = input.getPipeline();

		    PCollectionList<UserStatsRow> urowsList = PCollectionList.empty(pipe);
	        for (String inTable: bqTables) {
	        	String ym = inTable.substring(inTable.length()-7);
	        	PCollection<UserStatsRow> urows = pipe
					.apply("BQRead-"+ym, BigQueryIO.Read.from(inTable))
					.apply("Import-"+ym, MapElements
						.via(UserStatsRow::fromBQTableRow)
						.withOutputType(new TypeDescriptor<UserStatsRow>() {}));
				urowsList = urowsList.and(urows);
	        }

	        return urowsList.apply("Concat", Flatten.pCollections());
	    }

	    /**
	     * Returns list of table names that include date range dfrom (inclusive) - dto (inclusive)
	     */
		private static String[] tableNames(int serverId, LocalDate dfrom, LocalDate dto) {
			int yFrom = dfrom.isBefore(DATA_FROM) ? 2010 : dfrom.getYear();
			int mFrom = dfrom.isBefore(DATA_FROM) ? 1 : dfrom.getMonthOfYear();
			int yTo = dto.getYear();
			int mTo = dto.getMonthOfYear();

			ArrayList<String> bqTables = new ArrayList<>();
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

}