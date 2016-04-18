package com.modrykonik.dash.transforms;

import org.joda.time.DateTimeZone;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.model.LongPair;
import com.modrykonik.dash.model.UserStatsRow;

/**
 * Merge UserStats rows per [day, auth_user_id] using logical OR.
 */
public class OrMergeFn
	extends PTransform<PCollectionList<UserStatsRow>, PCollection<UserStatsRow>>
{

    @Override
    public PCollection<UserStatsRow> apply(PCollectionList<UserStatsRow> urowsList) {
	    //
	    PCollection<UserStatsRow> urowsMerged = urowsList
	    	//concat all urows into one collection
	    	//[PCollection<UserStatsRow>,PCollection<UserStatsRow>,...]  ->  PCollection<UserStatsRow>
	    	.apply("Concat", Flatten.pCollections())
	    	//extract key from each urow
	    	//PCollection<UserStatsRow>  ->  PCollection<KV<LongPair, UserStatsRow>>
        	.apply("ExtractKey", MapElements
                .via((UserStatsRow urow) -> KV.of(
            		new LongPair(
            			urow.day.toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis(),
        				urow.auth_user_id
        			),
            		urow
                ))
                .withOutputType(new TypeDescriptor<KV<LongPair, UserStatsRow>>() {}))
        	//merge urows per key
        	//PCollection<KV<LongPair, UserStatsRow>>  ->  PCollection<KV<LongPair, UserStatsRow>>
        	.apply("OrPerKey", Combine
    	    	.perKey((Iterable<UserStatsRow> urows) -> UserStatsRow.orMerge(urows)))
        	//drop key
        	//PCollection<KV<LongPair, UserStatsRow>>  ->  PCollection<UserStatsRow>>
        	.apply("DropKey", MapElements
                    .via((KV<LongPair, UserStatsRow> key_urow) -> key_urow.getValue())
                    .withOutputType(new TypeDescriptor<UserStatsRow>() {}));

	    return urowsMerged;
    }

}