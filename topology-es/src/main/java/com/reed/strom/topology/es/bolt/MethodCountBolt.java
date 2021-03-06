package com.reed.strom.topology.es.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MethodCountBolt extends BaseWindowedBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
			.getLogger(MethodCountBolt.class);

	private int sum = 0;
	private OutputCollector collector;
	private String methodName = null;
	private long ts = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuplesInWindow = inputWindow.get();
		List<Tuple> newTuples = inputWindow.getNew();
		List<Tuple> expiredTuples = inputWindow.getExpired();
		Map<String, Integer> counts = new HashMap<String, Integer>();

		for (Tuple tuple : newTuples) {
			methodName = (String) tuple.getValue(0);
			Integer count = counts.get(methodName);
			if (count == null) {
				count = 0;
			}
			count++;
			counts.put(methodName, count);
		}

		// Iterator iter = counts.entrySet().iterator();
		// while (iter.hasNext()) {
		// Map.Entry<String, Integer> entry = (Entry<String, Integer>)
		// iter.next();
		// this.jedis.zadd("s_server", entry.getValue(), entry.getKey());
		//
		// }
		//
		ts = System.currentTimeMillis();
		collector.emit(new Values(counts, ts, "method_name"));

		System.out.println("MethodCountBolt: Events in current window: "
				+ tuplesInWindow.size() + " timestamp: "
				+ System.currentTimeMillis());

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sumMap", "ts", "name"));
	}

}
