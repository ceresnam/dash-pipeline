package com.modrykonik.dash.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

/**
 * Serializable pair of two longs
 */
@DefaultCoder(AvroCoder.class)
public class LongPair {
	public long item1;
	public long item2;

	public LongPair() {}

	public LongPair(long item1, long item2) {
		this.item1 = item1;
		this.item2 = item2;
	}
}