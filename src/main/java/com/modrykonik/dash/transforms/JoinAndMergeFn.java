package com.modrykonik.dash.transforms;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * Join rows from two PCollections and apply merge() function on merged rows
 */
public abstract class JoinAndMergeFn<KeyT, InputT1, InputT2, OutputT>
	extends PTransform<PCollectionTuple, PCollection<OutputT>>
{
    private final TupleTag<InputT1> first;
    private final TupleTag<InputT2> second;
    private final Coder<KeyT> coderK;

    public JoinAndMergeFn(TupleTag<InputT1> tag1, TupleTag<InputT2> tag2, Coder<KeyT> coderK) {
    	this.first = tag1;
    	this.second = tag2;
    	this.coderK = coderK;
	}

    public abstract KeyT getKey1(InputT1 v1);
    public abstract KeyT getKey2(InputT2 v2);
    public abstract OutputT merge(InputT1 v1, InputT2 v2);

    @Override
    public PCollection<OutputT> apply(PCollectionTuple inputPair) {
	    PCollection<InputT1> vals1 = inputPair.get(first);
	    PCollection<InputT2> vals2 = inputPair.get(second);

	    //extract keys
	    //PCollection<<InputT1>  ->  PCollection<KV<KeyT,InputT1>>
	    PCollection<KV<KeyT,InputT1>> kvals1 = vals1
	    	.apply(MapElements
	    		.via((InputT1 v) -> KV.of(getKey1(v), v))
	    		.withOutputType(new TypeDescriptor<KV<KeyT,InputT1>>() {}))
	    	.setCoder(KvCoder.of(coderK, vals1.getCoder()));
  	    //PCollection<<InputT2>  ->  PCollection<KV<KeyT,InputT2>>
	    PCollection<KV<KeyT,InputT2>> kvals2 = vals2
	    	.apply(MapElements
	    		.via((InputT2 v) -> KV.of(getKey2(v), v))
	    		.withOutputType(new TypeDescriptor<KV<KeyT,InputT2>>() {}))
	    	.setCoder(KvCoder.of(coderK, vals2.getCoder()));

	    //join by key, and and per key for all pairs in cartesian product apply merge()
	    PCollection<OutputT> merged = KeyedPCollectionTuple.of(first, kvals1).and(second, kvals2)
	    	//PCollection<KV<KeyT,InputT1>>, PCollection<KV<KeyT,InputT2>>  ->
	    	//PCollection<KeyT,Iterable<InputT1>,Iterable<InputT2>>>
            .apply(CoGroupByKey.<KeyT>create())
            //PCollection<KeyT,Iterable<InputT1>,Iterable<InputT2>>>  ->  PCollection<OutputT>
            .apply(ParDo.of(
                new DoFn<KV<KeyT, CoGbkResult>, OutputT>() {
                	@Override
                	public void processElement(ProcessContext c) {
                		KV<KeyT, CoGbkResult> e = c.element();

                		// Get all collection 1 values
                		Iterable<InputT1> vals1 = e.getValue().getAll(first);
                		Iterable<InputT2> vals2 = e.getValue().getAll(second);

                		for (InputT1 v1: vals1) {
                			for (InputT2 v2: vals2) {
                				OutputT t = merge(v1, v2);
                				c.output(t);
                			}
                		}
                	}
                }));

	    return merged;
    }

}