package com.modrykonik.dash.transforms;

import com.modrykonik.dash.model.UserStatsComputedRow;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Merge UserStatsComputedRow rows per [day, auth_user_id] using logical OR.
 */
public class OrMergeFn
    extends PTransform<PCollectionList<UserStatsComputedRow>, PCollection<UserStatsComputedRow>>
{

    @SuppressWarnings({"UnnecessaryLocalVariable", "RedundantTypeArguments"})
    @Override
    public PCollection<UserStatsComputedRow> expand(PCollectionList<UserStatsComputedRow> urowsList) {
        PCollection<UserStatsComputedRow> ucrowsMerged = urowsList
            //concat all ucrows into one collection
            //[PCollection<UserStatsComputedRow>,PCollection<UserStatsComputedRow>,...]  ->
            //    PCollection<UserStatsComputedRow>
            .apply("Concat", Flatten.pCollections())
            //extract key from each ucrow
            //PCollection<UserStatsComputedRow>  ->  PCollection<KV<KV<Long, Long>, UserStatsComputedRow>>
            .apply("ExtractKey", MapElements
                .into(new TypeDescriptor<KV<KV<Long, Long>, UserStatsComputedRow>>() {})
                .via((UserStatsComputedRow ucrow) -> KV.of(
                    KV.of(
                        ucrow.day,
                        ucrow.auth_user_id
                    ),
                    ucrow
                )))
            //merge ucrows per key
            //PCollection<KV<KV<Long, Long>, UserStatsComputedRow>>  ->  PCollection<KV<KV<Long, Long>, UserStatsComputedRow>>
            .apply("OrPerKey", Combine.perKey(UserStatsComputedRow::orMerge))
            //drop key
            //PCollection<KV<KV<Long, Long>, UserStatsComputedRow>>  ->  PCollection<UserStatsComputedRow>>
            .apply("DropKey", MapElements
                .into(new TypeDescriptor<UserStatsComputedRow>() {})
                .via(KV<KV<Long, Long>, UserStatsComputedRow>::getValue));

        return ucrowsMerged;
    }

}
