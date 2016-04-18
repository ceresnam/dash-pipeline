package com.modrykonik.dash.io;

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.DashPipeline;
import com.modrykonik.dash.model.UserProperties;

/**
 * Read and write UserProperties from big query table
 */
public class BQUserPropsIO {

	/**
	 * Read UserProperties from big query table
	 */
	public static class Read
		extends PTransform<PInput, PCollection<UserProperties>>
	{
		private final String inTable;
		private final LocalDate dfrom;
		private final LocalDate dto;

		public Read(String bqTable, LocalDate dfrom, LocalDate dto) {
			this.inTable = bqTable;
			this.dfrom = dfrom;
			this.dto = dto;
		}

		public static Read from(int serverId) {
			return new Read(tableName(serverId, true), null, null);
		}

		public static Read from(int serverId, LocalDate dfrom, LocalDate dto) {
			return new Read(tableName(serverId, false), dfrom, dto);
		}

	    @Override
	    public PCollection<UserProperties> apply(PInput input) {
	    	Pipeline pipe = input.getPipeline();

	    	PCollection<TableRow> rows;
	    	if (dfrom!=null || dto!=null) {
	    		String cond1 = dfrom!=null ? String.format(
    				"registered_on >= '%s'",
    				dfrom.toDateTimeAtStartOfDay(DateTimeZone.UTC).toString()
    			) : "";
	    		String cond2 = dto!=null ? String.format(
	    			"registered_on < '%s'",
	    			dto.plusDays(1).toDateTimeAtStartOfDay(DateTimeZone.UTC).toString()
	    		) : "";
	    		String q = String.format("select * from %s where %s and %s", inTable, cond1, cond2);
	    		rows = pipe
				    .apply("BQRead", BigQueryIO.Read.fromQuery(q));
	    	} else {
	    		rows = pipe
				    .apply("BQRead", BigQueryIO.Read.from(inTable));
	    	}

	    	return rows
	    		.apply("Parse", MapElements
        			.via((TableRow row) -> UserProperties.from(row))
        			.withOutputType(new TypeDescriptor<UserProperties>() {}));
	    }
	}

	/**
	 * Write UserProperties to big query table
	 */
	public static class Write
		extends PTransform<PCollection<UserProperties>, PDone>
	{
		private final String outTable;

		public Write(String bqTable) {
			this.outTable = bqTable;
		}

		public static Write to(int serverId) {
			return new Write(tableName(serverId, true));
		}

		@Override
		public PDone apply(PCollection<UserProperties> input) {
			return input
		        .apply("Export", MapElements
					.via((UserProperties up) -> up.toBQTableRow())
                	.withOutputType(new TypeDescriptor<TableRow>() {}))
				.apply("BQWrite", BigQueryIO.Write
					.to(outTable)
					.withSchema(UserProperties.toBQTableSchema())
					.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
					.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		}
    }

	private static String tableName(int serverId, boolean withProject) {
		String t = String.format("user_stats_%d.ds_user_props", serverId);
		if (withProject) t = DashPipeline.PROJECT_ID + ':' + t;
		return t;
	}

}