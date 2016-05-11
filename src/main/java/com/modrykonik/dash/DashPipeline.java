package com.modrykonik.dash;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.modrykonik.dash.io.BQUserStatsComputedIO;
import com.modrykonik.dash.io.BQUserStatsIO;
import com.modrykonik.dash.model.UserStatsComputedRow;
import com.modrykonik.dash.model.UserStatsRow;
import com.modrykonik.dash.transforms.ComputeFeaturesFn;
import com.modrykonik.dash.transforms.OrMergeFn;
import com.modrykonik.dash.transforms.RollingBooleanFeatureFn;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Goole Cloud Dataflow pipeline for computing dash features
 *
 * build with:
 * 	   export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
 * 	   export PATH=$PATH:~/Downloads/apache-maven-3.3.9/bin
 *     mvn clean compile assembly:single
 *
 * run in cloud with:
 *     java -jar ./target/dash-pipeline-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 *         --project='maximal-beach-125109' --stagingLocation='gs://dash_import'
 *         --filesToStage=./target/dash-pipeline-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 *         --runner=DataflowPipelineRunner --autoscalingAlgorithm=THROUGHPUT_BASED --maxNumWorkers=5
 *         --serverid=202 --dfrom='2010-01-01' --dto='2010-12-31'
 */
public class DashPipeline {

	/**
	 * Our project ID in google cloud
	 */
    public static final String PROJECT_ID = "maximal-beach-125109";

	//private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	public static final DateTimeFormatter bqDatetimeFmt =
	    new DateTimeFormatterBuilder()
	    	.appendPattern("yyyy-MM-dd HH:mm:ss")
	    	.appendOptional(new DateTimeFormatterBuilder().appendLiteral('.').appendFractionOfSecond(1, 6).toParser())
	    	.appendLiteral(" UTC")
	    	.toFormatter().withZoneUTC();

    private static String toCamelCase(String s){
	    String camelCaseString = "";

		String[] parts = s.split("_");
		for (String part : parts){
			camelCaseString += part.substring(0, 1).toUpperCase() + part.substring(1).toLowerCase();
		}

		return camelCaseString;
	}

    @SafeVarargs
    private static <T> List<T> concat(T[]... ta) {
        return Stream.of(ta).flatMap(Stream::of).collect(Collectors.toList());
    }

	public static void main(String[] args) {

        try {
            AvroCoder.of(UserStatsComputedRow.class).verifyDeterministic();
        } catch (Coder.NonDeterministicException e) {
            e.printStackTrace();
            return;
        }

        PipelineOptionsFactory.register(DashPipelineOptions.class);
		DashPipelineOptions options = PipelineOptionsFactory.
			fromArgs(args).withValidation().as(DashPipelineOptions.class);

	    int serverId = options.getServerid();
	    LocalDate dfrom = LocalDate.parse(options.getDfrom());
	    LocalDate dto = LocalDate.parse(options.getDto());
	    assert dto.isAfter(dfrom) || dto.isEqual(dfrom);

	    DataflowPipelineOptions dfoptions = options.as(DataflowPipelineOptions.class);
	    String jobName = String.format("%s-%d-%s-%s-%s",
    		dfoptions.getAppName().toLowerCase(),
    		serverId,
    		dfrom.toString("yyyyMMdd"),
    		dto.toString("yyyyMMdd"),
    		// keep the same timestamp format as original value
    		DateTimeFormat.forPattern("MMddHHmmss").withZone(DateTimeZone.UTC).print(DateTimeUtils.currentTimeMillis())
	    );
	    dfoptions.setJobName(jobName);

	    Pipeline pipe = Pipeline.create(options);

	    // load from big query
	    LocalDate dfromPreload = dfrom.minusDays(90-1);  // need to read back in history for rollup features
		PCollection<UserStatsRow> inputData = pipe
			.apply("BQReadData", BQUserStatsIO.Read.from(serverId, dfromPreload, dto));

		//compute simple features
	    PCollection<UserStatsComputedRow> ucrowsInputData = inputData
	    	.apply("Compute", ParDo.of(new ComputeFeaturesFn()))
		    // make day from UserStats row as timestamp when the event occurred
		    .apply("SetEventTimestamps", WithTimestamps.of((UserStatsComputedRow r) ->
	    		r.day.toDateTimeAtStartOfDay(DateTimeZone.UTC).toInstant()
	    	));
        PCollectionList<UserStatsComputedRow> ucrowsResultsList = PCollectionList.empty(pipe);

			PCollection<UserStatsComputedRow> ucrows1d = ucrowsInputData
				.apply("IsActive1d", Filter.byPredicate((UserStatsComputedRow ucrow) ->
					(ucrow.day.isAfter(dfrom) || ucrow.day.isEqual(dfrom)) &&
					(ucrow.day.isBefore(dto) || ucrow.day.isEqual(dto))
				));
            ucrowsResultsList = ucrowsResultsList.and(ucrows1d);

		String[] rolling_7_28_90 = new String[] {
			"is_active", "is_alive", "has_registered"
		};
		String[] rolling_28 = new String[] {
			"is_bazar_active", "is_forum_active", "is_group_active", "is_photoblog_active",
			"is_bazar_alive", "is_forum_alive", "is_group_alive", "is_photoblog_alive",
		};

        for (String f: rolling_7_28_90) {
            String fin = f.equals("has_registered") ? "tmp_" + f : f;
            String fout = f+"7d";
            PCollection<UserStatsComputedRow> fucrows7d = ucrowsInputData
                .apply(toCamelCase(fout), new RollingBooleanFeatureFn(fin, fout, 7, dfrom, dto));
            ucrowsResultsList = ucrowsResultsList.and(fucrows7d);
        }

        for (String f: concat(rolling_7_28_90, rolling_28)) {
            String fin = f.equals("has_registered") ? "tmp_" + f : f;
            String fout = f+"28d";
            PCollection<UserStatsComputedRow> fucrows28d = ucrowsInputData
                .apply(toCamelCase(fout), new RollingBooleanFeatureFn(fin, fout, 28, dfrom, dto));
            ucrowsResultsList = ucrowsResultsList.and(fucrows28d);
        }

        for (String f: rolling_7_28_90) {
            String fin = f.equals("has_registered") ? "tmp_" + f : f;
            String fout = f+"90d";
            PCollection<UserStatsComputedRow> fucrows7d = ucrowsInputData
                .apply(toCamelCase(fout), new RollingBooleanFeatureFn(fin, fout, 90, dfrom, dto));
            ucrowsResultsList = ucrowsResultsList.and(fucrows7d);
        }

	    // merge all features into one ucrow per [day, auth_user_id]
        if (false) {
            //load existing data. If dWindow==1, table does not exist yet, so do not merge
            PCollection<UserStatsComputedRow> existingResults = pipe
                .apply("BQReadResults", BQUserStatsComputedIO.Read.from(serverId, dfrom, dto));
            ucrowsResultsList = ucrowsResultsList.and(existingResults);
        }

	    PCollection<UserStatsComputedRow> ucrowsResults = ucrowsResultsList
	    	.apply("OrMerge", new OrMergeFn());

	    // write to big query
        ucrowsResults
	    	.apply("BQWrite", BQUserStatsComputedIO.Write.to(serverId, dfrom, dto));

        pipe.run();
    }
}
