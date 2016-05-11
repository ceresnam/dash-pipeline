package com.modrykonik.dash.transforms;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.model.LongPair;
import com.modrykonik.dash.model.UserStatsComputedRow;
import org.joda.time.DateTimeZone;

/**
 * Merge UserStatsComputedRow rows per [day, auth_user_id] using logical OR.
 */
public class OrMergeFn
	extends PTransform<PCollectionList<UserStatsComputedRow>, PCollection<UserStatsComputedRow>>
{

    @SuppressWarnings({"UnnecessaryLocalVariable", "RedundantTypeArguments"})
	@Override
    public PCollection<UserStatsComputedRow> apply(PCollectionList<UserStatsComputedRow> urowsList) {
	    PCollection<UserStatsComputedRow> ucrowsMerged = urowsList
	    	//concat all ucrows into one collection
	    	//[PCollection<UserStatsComputedRow>,PCollection<UserStatsComputedRow>,...]  ->
            //    PCollection<UserStatsComputedRow>
	    	.apply("Concat", Flatten.pCollections())
	    	//extract key from each ucrow
	    	//PCollection<UserStatsComputedRow>  ->  PCollection<KV<LongPair, UserStatsComputedRow>>
        	.apply("ExtractKey", MapElements
                .via((UserStatsComputedRow ucrow) -> KV.of(
            		new LongPair(
            			ucrow.day.toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis(),
        				ucrow.auth_user_id
        			),
            		ucrow
                ))
                .withOutputType(new TypeDescriptor<KV<LongPair, UserStatsComputedRow>>() {}))
        	//merge ucrows per key
        	//PCollection<KV<LongPair, UserStatsComputedRow>>  ->  PCollection<KV<LongPair, UserStatsComputedRow>>
        	.apply("OrPerKey", Combine.perKey(UserStatsComputedRow::orMerge))
        	//drop key
        	//PCollection<KV<LongPair, UserStatsComputedRow>>  ->  PCollection<UserStatsComputedRow>>
        	.apply("DropKey", MapElements
				.via(KV<LongPair, UserStatsComputedRow>::getValue)
				.withOutputType(new TypeDescriptor<UserStatsComputedRow>() {}));

	    return ucrowsMerged;
    }

}