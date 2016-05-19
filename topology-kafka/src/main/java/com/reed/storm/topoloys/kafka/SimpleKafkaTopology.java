package com.reed.storm.topoloys.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reed.storm.utils.StormRunner;

public class SimpleKafkaTopology {

	public static class KafkaWordSplitter extends BaseRichBolt {

		private static final Logger LOG = LoggerFactory
				.getLogger(KafkaWordSplitter.class);
		private static final long serialVersionUID = 886149197481637894L;
		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void execute(Tuple input) {
			String line = input.getString(0);
			LOG.info("RECV[kafka -> splitter] " + line);
			String[] words = line.split("\\s+");
			for (String word : words) {
				LOG.info("EMIT[splitter -> counter] " + word);
				collector.emit(input, new Values(word, 1));
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}

	}

	public static class WordCounter extends BaseRichBolt {

		private static final Logger LOG = LoggerFactory
				.getLogger(WordCounter.class);
		private static final long serialVersionUID = 886149197481637894L;
		private OutputCollector collector;
		private Map<String, AtomicInteger> counterMap;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
			this.counterMap = new HashMap<String, AtomicInteger>();
		}

		@Override
		public void execute(Tuple input) {
			String word = input.getString(0);
			int count = input.getInteger(1);
			LOG.info("RECV[splitter -> counter] " + word + " : " + count);
			AtomicInteger ai = this.counterMap.get(word);
			if (ai == null) {
				ai = new AtomicInteger();
				this.counterMap.put(word, ai);
			}
			ai.addAndGet(count);
			collector.ack(input);
			LOG.info("CHECK statistics map: " + this.counterMap);
		}

		@Override
		public void cleanup() {
			LOG.info("The final result:");
			Iterator<Entry<String, AtomicInteger>> iter = this.counterMap
					.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, AtomicInteger> entry = iter.next();
				LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException,
			AuthorizationException {
		String zks = "172.28.20.103:2181,172.28.20.104:2181,172.28.20.105:2181";
		String topic = "test";
		String zkRoot = "/storm"; // default zookeeper root configuration for
									// storm
		String id = SimpleKafkaTopology.class.getSimpleName();

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.zkServers = Arrays.asList(new String[] { "172.28.20.103",
				"172.28.20.104", "172.28.20.105" });
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
		builder.setBolt("word-splitter", new KafkaWordSplitter(), 2)
				.shuffleGrouping("kafka-reader");
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping(
				"word-splitter", new Fields("word"));

		Config conf = new Config();

		String name = SimpleKafkaTopology.class.getSimpleName();
		// remote
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormRunner.runTopologyRemotely(builder.createTopology(), name,
					conf);
			// StormSubmitter.submitTopologyWithProgressBar(name, conf,
			// builder.createTopology());
		} else {
			// local
			conf.setMaxTaskParallelism(3);
			// LocalCluster cluster = new LocalCluster();
			// cluster.submitTopology(name, conf, builder.createTopology());
			// Thread.sleep(60000);
			// cluster.killTopology(name);
			// cluster.shutdown();
			// System.exit(1);
			StormRunner.runTopologyLocally(builder.createTopology(), name,
					conf, 120);
		}
	}
}