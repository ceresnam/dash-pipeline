package com.modrykonik.dash;

import com.google.api.services.bigquery.model.TableRow;
import com.modrykonik.dash.io.BQUserStatsIO;
import com.modrykonik.dash.model.DateUtils;
import com.modrykonik.dash.model.UserStatsComputedRow;
import com.modrykonik.dash.model.UserStatsRow;
import com.modrykonik.dash.transforms.BQMergeFn;
import com.modrykonik.dash.transforms.ComputeFeaturesFn;
import com.modrykonik.dash.transforms.OrMergeFn;
import com.modrykonik.dash.transforms.RollingBooleanFeatureFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.LocalDate;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.modrykonik.dash.io.BQUtils.parseDate;
import static com.modrykonik.dash.model.DateUtils.isBetween;
import static com.modrykonik.dash.model.DateUtils.toLocalDate;

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

    private static String toCamelCase(String s){
        String camelCaseString = "";

        String[] parts = s.split("_");
        for (String part : parts){
            camelCaseString += part.substring(0, 1).toUpperCase() + part.substring(1).toLowerCase();
        }

        return camelCaseString;
    }

    static {
        PipelineOptionsFactory.register(DashPipelineOptions.class);
    }

    @SafeVarargs
    private static <T> List<T> concat(T[]... ta) {
        return Stream.of(ta).flatMap(Stream::of).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        DashPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DashPipelineOptions.class);
        runPipeline(options);
    }

    public static void runPipeline(DashPipelineOptions options) {
        ValueProvider<Integer> serverId = options.getServerid();
        ValueProvider<LocalDate> dfrom = NestedValueProvider.of(options.getDfrom(), DateUtils::fromStr);
        ValueProvider<LocalDate> dto = NestedValueProvider.of(options.getDto(), DateUtils::fromStr);

        Pipeline pipe = Pipeline.create(options);

        // load from big query
        ValueProvider<LocalDate> dfromPreload = // need to read back in history for rollup features
            NestedValueProvider.of(dfrom, (LocalDate d) -> d.minusDays(90-1));
        PCollection<TableRow> inputAndPreloadData = pipe
            .apply("BQRead", BQUserStatsIO.Read.from(serverId, dfromPreload, dto));

        // separate input data only (drop rows preloaded for rolling computation)
        PCollection<TableRow> inputData = inputAndPreloadData
            .apply("BQFilter", Filter.by(
                (trow) -> isBetween(parseDate(trow, "day"), dfrom.get(), dto.get())
            ));

        //compute simple features
        PCollection<UserStatsComputedRow> ucrowsInputData = inputAndPreloadData
            .apply("BQImport", MapElements
                .into(new TypeDescriptor<UserStatsRow>() {})
                .via(UserStatsRow::fromBQTableRow))
            .apply("Compute", ParDo.of(new ComputeFeaturesFn()));
        PCollectionList<UserStatsComputedRow> ucrowsResultsList = PCollectionList.empty(pipe);

        PCollection<UserStatsComputedRow> ucrows1d = ucrowsInputData
            .apply("IsActive1d", Filter.by(
                (UserStatsComputedRow ucrow) -> isBetween(toLocalDate(ucrow.day), dfrom.get(), dto.get())
            ));
        ucrowsResultsList = ucrowsResultsList.and(ucrows1d);

        String[] rolling_7_28_90 = new String[] {
            "is_active", "is_alive", "has_registered"
        };
        String[] rolling_28 = new String[] {
            "is_bazar_active", "is_forum_active", "is_group_active", "is_photoblog_active",
            "is_bazar_alive", "is_forum_alive", "is_group_alive", "is_photoblog_alive",
            "is_desktop", "is_mobile",
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
        PCollection<UserStatsComputedRow> ucrowsResults = ucrowsResultsList
            .apply("OrMerge", new OrMergeFn());

        PCollection<TableRow> computedData = ucrowsResults
            .apply("BQExport", MapElements
                .into(new TypeDescriptor<TableRow>() {})
                .via(UserStatsComputedRow::toBQTableRow));

        PCollectionList<TableRow> allData = PCollectionList.of(inputData);
        //noinspection ConstantConditions,ConstantIfStatement
        if (false) {
            //optionally, merge with existing data
            PCollection<TableRow> existingData = pipe
                .apply("BQReadExisting", BQUserStatsIO.Read.from(serverId, dfrom, dto, "all"));
            allData = allData.and(existingData);
        }
        allData = allData.and(computedData);

        // write to big query
        allData
            .apply("BQMerge", new BQMergeFn())
            .apply("BQWrite", BQUserStatsIO.Write.to(serverId, "all"));

        pipe.run();
    }
}
