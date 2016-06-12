package com.reed.storm.topology.rolling.bolt;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.WindowsState;
import org.apache.storm.tuple.Values;

public class WindowMapGet extends BaseQueryFunction<WindowsState, Object> {
	public List<Object> batchRetrieve(WindowsState map, List<TridentTuple> keys) {
		List<Object> list = new ArrayList<Object>();
		for (TridentTuple t : keys) {
			if (t != null) {
				list.add(t.getValues());
			}
		}
		return list;
	}

	public void execute(TridentTuple tuple, Object result,
			TridentCollector collector) {
		collector.emit(new Values(new Object[] { result }));
	}
}
