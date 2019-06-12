package com.modrykonik.dash.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.modrykonik.dash.io.BQUtils;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

import static com.modrykonik.dash.io.BQUtils.parseDateMillis;
import static com.modrykonik.dash.io.BQUtils.parseLong;

/**
 * Merge TableRows rows per [day, auth_user_id].
 */
public class BQMergeFn
    extends PTransform<PCollectionList<TableRow>, PCollection<TableRow>>
{

    @SuppressWarnings({"UnnecessaryLocalVariable", "RedundantTypeArguments"})
    @Override
    public PCollection<TableRow> expand(PCollectionList<TableRow> rowsList) {
        PCollection<TableRow> rowsMerged = rowsList
            //concat all rows into one collection
            //[PCollection<TableRow>,PCollection<TableRow>,...]  -> PCollection<TableRow>
            .apply("Concat", Flatten.pCollections())
            //extract key from each row
            //PCollection<TableRow>  ->  PCollection<KV<KV<Long, Long>, TableRow>>
            .apply("ExtractKey", MapElements
                .into(new TypeDescriptor<KV<KV<Long,Long>, TableRow>>() {})
                .via((TableRow trow) -> KV.of(
                    KV.of(
                        parseDateMillis(trow, "day"),
                        parseLong(trow, "auth_user_id")
                    ),
                    trow
                )))
            //merge rows per key
            //PCollection<KV<KV<Long, Long>, TableRow>>  ->  PCollection<KV<KV<Long, Long>, TableRow>>
            .apply("Merge", Combine.perKey(BQUtils::mergeTableRows))
            //drop key
            //PCollection<KV<KV<Long, Long>, TableRow>>  ->  PCollection<TableRow>>
            .apply("DropKey", MapElements
                .into(new TypeDescriptor<TableRow>() {})
                .via(KV<KV<Long,Long>,TableRow>::getValue));

        return rowsMerged;
    }

}
