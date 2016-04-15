package com.modrykonik.dash.io;

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

		public Read(String bqTable) {
			this.inTable = bqTable;
		}

		public static Read from(int serverId) {
			return new Read(tableName(serverId));
		}

	    @Override
	    public PCollection<UserProperties> apply(PInput input) {
	    	Pipeline pipe = input.getPipeline();

	    	return pipe
	    		.apply("BQRead", BigQueryIO.Read.from(inTable))
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
			return new Write(tableName(serverId));
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

	private static String tableName(int serverId) {
		return String.format("%s:user_stats_%d.ds_user_props", DashPipeline.PROJECT_ID, serverId);
	}
}