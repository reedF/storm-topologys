package com.reed.storm.topology.rolling.bolt;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.reed.storm.topology.rolling.bolt.CountMapAsAggregator.CountState;

public class CountMapAsAggregator extends BaseAggregator<CountState> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3476873895955573242L;

	static class CountState extends Object {
		long count = 0L;
		String key;
	}

	public CountState init(Object paramObject,
			TridentCollector paramTridentCollector) {
		return new CountState();
	}

	public void aggregate(CountState state, TridentTuple tuple,
			TridentCollector paramTridentCollector) {
		state.count += 1;
		state.key = tuple.getString(0);

	}

	public void complete(CountState state, TridentCollector collector) {
		collector.emit(new Values(state.key, state.count));

	}

}
