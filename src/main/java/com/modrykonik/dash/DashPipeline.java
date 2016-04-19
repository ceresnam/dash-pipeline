package com.modrykonik.dash;

import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.io.BQUserPropsIO;
import com.modrykonik.dash.io.BQUserStatsIO;
import com.modrykonik.dash.model.UserProperties;
import com.modrykonik.dash.model.UserStatsRow;
import com.modrykonik.dash.transforms.OrMergeFn;
import com.modrykonik.dash.transforms.RollingBooleanFeatureFn;

/**
 * Goole Cloud Dataflow pipeline for computing dash features
 *
 * build with:
 *     mvn clean compile assembly:single
 *
 * run in cloud with:
 *     java -jar ./target/dash-pipeline-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 *         --serverid=202 --dfrom='2010-01-01' --dto='2010-12-31'
 *         --project='maximal-beach-125109' --stagingLocation='gs://dash_import'
 *         --runner=DataflowPipelineRunner --autoscalingAlgorithm=THROUGHPUT_BASED --maxNumWorkers=5
 */
@SuppressWarnings("serial")
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

	/**
	 * Import from BQ table row and computed features that depend only on data from BQ
	 */
	static class ImportFromBQAndComputeFeaturesFn extends DoFn<TableRow, UserStatsRow> {

	    /**
		 * Extract BOOLEAN value from BQ table cell. Return 0 if null in BQ table
		 */
		private boolean parseBoolean(TableRow row, String colName) {
			Boolean val = (Boolean) row.get(colName);
			return val!=null ? val.booleanValue() : false;
		}

		/**
		 * Extract INTEGER value from BQ table cell. Return 0 if null in BQ table
		 */
		private long parseLong(TableRow row, String colName) {
			String val = (String) row.get(colName);
			return val!=null ? Long.parseLong(val) : 0;
		}

		/**
		 * Return true if one of columns in BQ table row has value > 0
		 */
		private boolean oneOf(TableRow row, String... colNames) {
			for (String colName : colNames) {
				long val = parseLong(row, colName);
				if (val>0) return true;
			}
			return false;
		}

		@Override
		public void processElement(ProcessContext c) {
			TableRow row = c.element();

			UserStatsRow urow = new UserStatsRow();
			urow.day = Instant.parse((String) row.get("day"), bqDatetimeFmt).toDateTime().toLocalDate();
			urow.auth_user_id = Long.parseLong((String) row.get("auth_user_id"));
			TableRow data = (TableRow) row.get("data");

			// Blogy - aktivita
			urow.is_photoblog_active = oneOf(data,
				"num_photoblog_posts",
				"num_photoblog_comments",
				"num_photoblog_likes_given",
				"num_photoblog_likes_given_post"
			);

			// Skupiny - aktivita
			urow.is_group_active = oneOf(data,
				"num_group_joined",
				"num_group_posts",
				"num_group_post_comments",
				"num_group_likes_given_post",
				"num_groups"
			);

			// Blogy a Skupiny - aktivita
			urow.is_pbandgroup_active =
				urow.is_photoblog_active ||
				urow.is_group_active;

			// Fórum - aktivita
			urow.is_forum_active = oneOf(data,
				"num_forum_threads",
				"num_forum_messages",
				"num_forum_likes_given_thread",
				"num_forum_likes_given_message"
			);

	        // Bazár - aktivita
			urow.is_bazar_active = oneOf(data,
				"num_bazar_products",
				"num_bazar_products_reposted",
				"num_bazar_reviews",
				"num_bazar_transaction_message_to_seller",
				"num_bazar_transaction_message_to_buyer",
				"num_bazar_interest_made",
				"num_bazar_wishlist_added",
				"num_bazar_likes_given"
	        );

			// Wiki - aktivita
			urow.is_wiki_active = oneOf(data,
	            "num_wiki_experiences",
	            "num_wiki_likes_given_experience"
	        );

			// IP - aktivita
			urow.is_ip_active = oneOf(data,
	            "num_ip_sent",
	            "num_ip_starred"
	        );

			// Srdiečka - aktivita
			urow.is_hearts_active = oneOf(data,
		        "num_hearts_given"
	        );

			//AARRR - aktivita na stránke
			urow.is_active =
				urow.is_photoblog_active ||
				urow.is_group_active ||
				urow.is_forum_active ||
				urow.is_bazar_active ||
				urow.is_wiki_active ||
				urow.is_ip_active ||
				urow.is_hearts_active;

			//AARRR - alive na stránke
			// kym sme nezbierali num_minutes_on_site, tak
			// za is_alive povazuj aj ked spravil login alebo aktivnu akciu
			urow.is_alive = oneOf(data,
	            "num_logins",
	            "num_minutes_on_site"
	        ) || urow.is_active;

			urow.is_bazar_alive = oneOf(data, "num_minutes_on_site_forum") || urow.is_bazar_active;
			urow.is_forum_alive = oneOf(data, "num_minutes_on_site_bazar") || urow.is_forum_active;
			urow.is_group_alive = oneOf(data, "num_minutes_on_site_group") || urow.is_group_active;
			urow.is_photoblog_alive = oneOf(data, "num_minutes_on_site_photoblog") || urow.is_photoblog_active;

			urow.has_pregnancystate_pregnant = parseBoolean(row, "has_pregnancystate_pregnant");
			urow.has_pregnancystate_trying = parseBoolean(row, "has_pregnancystate_trying");
			urow.has_profile_avatar = parseBoolean(row, "has_profile_avatar");
			urow.has_profile_birthdate = parseBoolean(row, "has_profile_birthdate");
			urow.has_profile_child = parseBoolean(row, "has_profile_child");
			urow.has_profile_city = parseBoolean(row, "has_profile_city");
			urow.has_profile_county = parseBoolean(row, "has_profile_county");

			c.output(urow);
		}
	}

	/**
	 * Compute features that depend on already computed rolling features computed
	 */
	static class ComputeDepFeaturesFn extends DoFn<UserStatsRow, UserStatsRow> {

		@Override
		public void processElement(ProcessContext c)
			throws Exception
		{
			UserStatsRow urow = (UserStatsRow) c.element().clone();

			urow.has_pregnancystate_pregnant_active28d =
				urow.has_pregnancystate_pregnant || urow.is_active28d;
			urow.has_pregnancystate_trying_active28d =
				urow.has_pregnancystate_trying || urow.is_active28d;
			urow.has_profile_avatar_active28d =
				urow.has_profile_avatar || urow.is_active28d;
			urow.has_profile_birthdate_active28d =
				urow.has_profile_birthdate || urow.is_active28d;
			urow.has_profile_child_active28d =
				urow.has_profile_child || urow.is_active28d;
			urow.has_profile_city_active28d =
				urow.has_profile_city || urow.is_active28d;
			urow.has_profile_county_active28d =
				urow.has_profile_county || urow.is_active28d;

			urow.has_pregnancystate_pregnant_alive28d =
					urow.has_pregnancystate_pregnant || urow.is_alive28d;
			urow.has_pregnancystate_trying_alive28d =
				urow.has_pregnancystate_trying || urow.is_alive28d;
			urow.has_profile_avatar_alive28d =
				urow.has_profile_avatar || urow.is_alive28d;
			urow.has_profile_birthdate_alive28d =
				urow.has_profile_birthdate || urow.is_alive28d;
			urow.has_profile_child_alive28d =
				urow.has_profile_child || urow.is_alive28d;
			urow.has_profile_city_alive28d =
				urow.has_profile_city || urow.is_alive28d;
			urow.has_profile_county_alive28d =
				urow.has_profile_county || urow.is_alive28d;

			c.output(urow);
		}
	}

	static String toCamelCase(String s){
	    String camelCaseString = "";

		String[] parts = s.split("_");
		for (String part : parts){
			camelCaseString += part.substring(0, 1).toUpperCase() + part.substring(1).toLowerCase();
		}

		return camelCaseString;
	}

	public static void main(String[] args) {
		PipelineOptionsFactory.register(DashPipelineOptions.class);
		DashPipelineOptions options = PipelineOptionsFactory.
			fromArgs(args).withValidation().as(DashPipelineOptions.class);
	    Pipeline pipe = Pipeline.create(options);

	    int serverId = options.getServerid();
	    LocalDate dfrom = LocalDate.parse(options.getDfrom());
	    LocalDate dto = LocalDate.parse(options.getDto());
	    assert dto.isAfter(dfrom) || dto.isEqual(dfrom);

	    // load from big query and compute simple features
	    LocalDate dfromPreload = dfrom.minusDays(90-1);  // need to read back in history for rollup features
	    PCollection<UserStatsRow> urowsData = pipe
    		.apply("BQRead", BQUserStatsIO.Read.from(serverId, dfromPreload, dto))
	    	.apply("BQImportAndCompute", ParDo.of(new ImportFromBQAndComputeFeaturesFn()))
		    // make day from UserStats row as timestamp when the event occurred
		    .apply("SetEventTimestamps", WithTimestamps.of((UserStatsRow r) ->
	    		r.day.toDateTimeAtStartOfDay(DateTimeZone.UTC).toInstant()
	    	));

	    PCollection<UserStatsRow> urowsProp = pipe
	    	.apply("ReadUserProps", BQUserPropsIO.Read.from(serverId, dfromPreload, dto))
	    	.apply("FilterRegistered", Filter.byPredicate((UserProperties uprop) -> {
		    		LocalDate registered_on = uprop.registered_on.toDateTime(DateTimeZone.UTC).toLocalDate();
		    		return
		    		    (registered_on.isAfter(dfromPreload) || registered_on.isEqual(dfromPreload)) &&
		    		    (registered_on.isBefore(dto) || registered_on.isEqual(dto));
	    		}))
		    .apply("IsRegistered", MapElements
    			.via((UserProperties uprop) -> {
	    				UserStatsRow urow = new UserStatsRow();
	    				urow.auth_user_id = uprop.auth_user_id;
	    				urow.day = uprop.registered_on.toDateTime(DateTimeZone.UTC).toLocalDate();
	    				urow.is_register = true;
	    				return urow;
	    			})
    			.withOutputType(new TypeDescriptor<UserStatsRow>() {}))
		    // make day from UserStats row as timestamp when the event occurred
		    .apply("SetEventTimestampsProps", WithTimestamps.of((UserStatsRow r) ->
	    		r.day.toDateTimeAtStartOfDay(DateTimeZone.UTC).toInstant()
	    	));

	    PCollection<UserStatsRow> urows1 = urowsData
	    	.apply("IsActive1d", Filter.byPredicate((UserStatsRow urow) ->
	    		(urow.day.isAfter(dfrom) || urow.day.isEqual(dfrom)) &&
	    		(urow.day.isBefore(dto) || urow.day.isEqual(dto))
	    	));

	    // compute rolling features
	    PCollectionList<UserStatsRow> urowsList = PCollectionList.of(urows1);
	    String[] rolling_7_28_90 = new String[] {
	    	"is_active", "is_alive"
	    };
	    for (String f: rolling_7_28_90) {
	    	PCollection<UserStatsRow> furows7d = urowsData
				 .apply(toCamelCase(f+"7d"), new RollingBooleanFeatureFn(f, f+"7d", 7, dfrom, dto));
	    	PCollection<UserStatsRow> furows28d = urowsData
			    .apply(toCamelCase(f+"28d"), new RollingBooleanFeatureFn(f, f+"28d", 28, dfrom, dto));
	    	PCollection<UserStatsRow> furows90d = urowsData
				 .apply(toCamelCase(f+"90d"), new RollingBooleanFeatureFn(f, f+"90d", 90, dfrom, dto));
	    	urowsList = urowsList.and(furows7d).and(furows28d).and(furows90d);
	    }
	    String[] rolling_28 = new String[] {
    		"is_bazar_active", "is_forum_active", "is_group_active", "is_photoblog_active",
    		"is_bazar_alive", "is_forum_alive", "is_group_alive", "is_photoblog_alive",
	    };
	    for (String f: rolling_28) {
	    	PCollection<UserStatsRow> furows7d = urowsData
			    .apply(toCamelCase(f+"28d"), new RollingBooleanFeatureFn(f, f+"28d", 28, dfrom, dto));
	    	urowsList = urowsList.and(furows7d);
	    }
	    String[] rollingProp_7_28_90 = new String[] {
		    	"is_register"
		    };
		    for (String f: rollingProp_7_28_90) {
		    	PCollection<UserStatsRow> furows7d = urowsProp
					 .apply(toCamelCase(f+"7d"), new RollingBooleanFeatureFn(f, f+"7d", 7, dfrom, dto));
		    	PCollection<UserStatsRow> furows28d = urowsProp
				    .apply(toCamelCase(f+"28d"), new RollingBooleanFeatureFn(f, f+"28d", 28, dfrom, dto));
		    	PCollection<UserStatsRow> furows90d = urowsProp
					 .apply(toCamelCase(f+"90d"), new RollingBooleanFeatureFn(f, f+"90d", 90, dfrom, dto));
		    	urowsList = urowsList.and(furows7d).and(furows28d).and(furows90d);
		    }

	    // merge all features into one urow per [day, auth_user_id]
	    PCollection<UserStatsRow> urowsMerged = urowsList
	    	.apply("OrMerge", new OrMergeFn());

	    //compute features that depend on rolling features computed before
	    urowsMerged = urowsMerged
	    	.apply("ComputeDep", ParDo.of(new ComputeDepFeaturesFn()));
	    //PCollection<UserStatsRow> urowsMerged = urows1;

	    // write to big query
	    urowsMerged
	    	.apply("BQWrite", BQUserStatsIO.Write.to(serverId, dfrom, dto));

        pipe.run();
    }
}
