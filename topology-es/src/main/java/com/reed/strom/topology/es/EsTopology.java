package com.reed.strom.topology.es;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.trident.spout.RichSpoutBatchExecutor;

import com.reed.strom.topology.es.bolt.ClassCountBolt;
import com.reed.strom.topology.es.bolt.CommonEsBolt;
import com.reed.strom.topology.es.bolt.DispatchBolt;
import com.reed.strom.topology.es.bolt.GeneralInfoBolt;
import com.reed.strom.topology.es.bolt.KafkaWordSplitter;
import com.reed.strom.topology.es.bolt.MethodCountBolt;
import com.reed.strom.topology.es.bolt.ProjectCountBolt;
import com.reed.strom.topology.es.bolt.ResultCodeSummaryBolt;

public class EsTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException,
			InterruptedException {
		String zks = "172.28.18.209:2181,172.28.18.210:2181,172.28.18.211:2181,172.28.18.219:2181,172.28.18.220:2181";
		String topic = "test";
		String zkRoot = "/storm"; // default zookeeper root configuration for
									// storm
		String id = EsTopology.class.getSimpleName();
		String uuid = UUID.randomUUID().toString();

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot,
				"sumcount3");
		// SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot,
		// "EsID");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.zkServers = Arrays.asList(new String[] { "172.28.18.209",
				"172.28.18.210", "172.28.18.211", "172.28.18.219",
				"172.28.18.220" });
		spoutConf.zkPort = 2181;
		spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

		// KafkaConfig kafkaConf = new KafkaConfig(brokerHosts, uuid);
		// kafkaConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

		BaseWindowedBolt projectBolt = new ProjectCountBolt().withWindow(
				new Duration(10, TimeUnit.SECONDS), new Duration(10,
						TimeUnit.SECONDS));
		// .withTimestampField("ts")
		// .withWatermarkInterval(new Duration(1, TimeUnit.SECONDS))
		// .withLag(new Duration(5, TimeUnit.SECONDS));

		BaseWindowedBolt classBolt = new ClassCountBolt().withWindow(
				new Duration(10, TimeUnit.SECONDS), new Duration(10,
						TimeUnit.SECONDS));
		// .withTimestampField("ts")
		// .withWatermarkInterval(new Duration(1, TimeUnit.SECONDS))
		// .withLag(new Duration(5, TimeUnit.SECONDS));

		BaseWindowedBolt methodBolt = new MethodCountBolt().withWindow(
				new Duration(10, TimeUnit.SECONDS), new Duration(10,
						TimeUnit.SECONDS));
		// .withTimestampField("ts")
		// .withWatermarkInterval(new Duration(1, TimeUnit.SECONDS))
		// .withLag(new Duration(5, TimeUnit.SECONDS));

		BaseWindowedBolt resultCodeBolt = new ResultCodeSummaryBolt()
				.withWindow(new Duration(10, TimeUnit.SECONDS), new Duration(
						10, TimeUnit.SECONDS));

		BaseWindowedBolt generalInfoBolt = new GeneralInfoBolt().withWindow(
				new Duration(10, TimeUnit.SECONDS), new Duration(10,
						TimeUnit.SECONDS));

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 6);
		builder.setBolt("word-splitter", new KafkaWordSplitter(), 6)
				.shuffleGrouping("kafka-reader");
		/* 根据streamID将tuple分发到不同的bolt */
		builder.setBolt("dispatch", new DispatchBolt(), 6).shuffleGrouping(
				"word-splitter", "splitByNameID");
		builder.setBolt("generalInfo-count", generalInfoBolt, 2)
				.shuffleGrouping("dispatch", "GeneralStreamID");

		// builder.setBolt("emit2es", new Write2EsBolt(),
		// 6).shuffleGrouping("project-count").shuffleGrouping("class-count").shuffleGrouping("method-count");
		builder.setBolt("common-es", new CommonEsBolt(), 2).shuffleGrouping(
				"generalInfo-count");

		Config conf = new Config();

		if ((args != null) && (args.length > 0)) {
			conf.setNumWorkers(3);
			conf.setMaxSpoutPending(5000);
			conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, 64 * 1024);
			StormSubmitter.submitTopology("EsTopology", conf,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("EsTopology", conf, builder.createTopology());
			Thread.sleep(140 * 1000);
			cluster.killTopology("EsTopology");
			cluster.shutdown();
			System.exit(1);
		}
	}
}
