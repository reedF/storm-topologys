package com.reed.storm.topology.rolling.bolt;

import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.tuple.TridentTuple;

public class NowCount extends Count {

	@Override
	public Long init(TridentTuple tuple) {
		return tuple.size() < 2 ? tuple.getLong(0) : tuple.getLong(1);
	}

	@Override
	public Long combine(Long val1, Long val2) {
		return val2;
	}

}
