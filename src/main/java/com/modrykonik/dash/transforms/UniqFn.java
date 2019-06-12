package com.modrykonik.dash.transforms;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Filter out repeated items from a PCollection
 */
public class UniqFn<T>
    extends PTransform<PCollection<T>, PCollection<T>>
{
    @SuppressWarnings("RedundantTypeArguments")
    @Override
    public PCollection<T> expand(PCollection<T> items) {
        return items
            //PCollection<T> -> PCollection<KV<T,Long>>
            .apply(Count.perElement())
            //PCollection<KV<T, Long>> -> PCollection<T>
            .apply(MapElements
                .into(new TypeDescriptor<T>() {})
                .via(KV<T, Long>::getKey))
            .setCoder(items.getCoder());
    }
}
