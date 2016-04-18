package com.modrykonik.dash.transforms;

import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * Filter out repeated items from a PCollection
 */
public class UniqFn<T>
	extends PTransform<PCollection<T>, PCollection<T>>
{
    @Override
    public PCollection<T> apply(PCollection<T> items) {
    	return items
    		//PCollection<T> -> PCollection<KV<T,Long>>
    		.apply(Count.<T>perElement())
    		//PCollection<KV<T, Long>> -> PCollection<T>
    		.apply(MapElements
    			.via((KV<T, Long> t_count) -> t_count.getKey())
    			.withOutputType(new TypeDescriptor<T>() {}))
    		.setCoder(items.getCoder());
    }
}