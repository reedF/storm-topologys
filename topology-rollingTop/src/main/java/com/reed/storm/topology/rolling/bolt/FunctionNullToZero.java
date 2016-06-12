package com.reed.storm.topology.rolling.bolt;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class FunctionNullToZero extends BaseFunction {
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Long word = tuple.getLong(0);
		if (word == null) {
			word = 0l;
		}
		collector.emit(new Values(word));
	}
}
