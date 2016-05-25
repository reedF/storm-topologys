package com.reed.storm.topology.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reed.storm.topology.drpc.bolt.ExclaimBolt;
import com.reed.storm.utils.StormRunner;

/**
 * DRPC topology，使用DRPC模式提供实时统计数据，给DRPC-client实时调用查询
 * @author reed
 *
 */
public class DrpcTopology {
	private static final Logger LOG = LoggerFactory
			.getLogger(DrpcTopology.class);
	private static final String topologyName = DrpcTopology.class
			.getSimpleName();
	public static final String functionName = "drpcFunc";

	private static final String zkUrl = "172.28.20.103:2181,172.28.20.104:2181,172.28.20.105:2181";
	private static final String topic = "test";

	public OpaqueTridentKafkaSpout createKafkaSpout() {
		String clientId = DrpcTopology.class.getSimpleName();
		ZkHosts hosts = new ZkHosts(zkUrl);
		TridentKafkaConfig config = new TridentKafkaConfig(hosts, topic,
				clientId);
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		// Consume new data from the topic
		config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return new OpaqueTridentKafkaSpout(config);
	}

	public TridentState addTridentState(TridentTopology tridentTopology) {
		String txId = DrpcTopology.class.getSimpleName();
		return tridentTopology
				.newStream(txId, createKafkaSpout())
				.parallelismHint(2)
				.each(new Fields("str"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(),
						new Fields("count")).parallelismHint(2);
	}

	/**
	 * DRPC简单使用，统计kafka topic内wordcount
	 * @return
	 */
	public TridentTopology wordCount() {
		TridentTopology tridentTopology = new TridentTopology();
		TridentState state = addTridentState(tridentTopology);
		tridentTopology
				.newDRPCStream(functionName)
				.each(new Fields("args"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.stateQuery(state, new Fields("word"), new MapGet(),
						new Fields("count"))
				.each(new Fields("count"), new FilterNull())
				.project(new Fields("word", "count"));
		return tridentTopology;
	}

	/**
	 * 校验打印启动参数
	 * @param args
	 */
	public void checkParameter(String[] args) {
		StringBuffer sf = new StringBuffer("=============");
		if (args != null) {
			for (String s : args) {
				if (s != null) {
					sf.append(s);
				}
			}
		}
		LOG.info(sf.toString());
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException {
		DrpcTopology t = new DrpcTopology();
		Config conf = new Config();
		conf.setNumWorkers(2);
		t.checkParameter(args);
		// remote
		if (args == null || args.length == 0) {
			StormRunner.runTopologyRemotely(t.wordCount().build(),
					topologyName, conf);
		} else if ("local".equals(args[0])) {
			// local
			LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
					functionName);
			builder.addBolt(new ExclaimBolt(), 3);
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf,
					builder.createLocalTopology(drpc));
			String str = "hello";
			for (int i = 0; i < 10; i++) {
				String r = drpc.execute(functionName, str);
				LOG.info(">>>>>>>>>>>>>>>>Results:" + r);
				str = r;
			}
			cluster.shutdown();
			drpc.shutdown();
			System.exit(1);
		} else {
			LOG.error(">>>>>>>>>ERROR:paramters for main method are wrong!");
		}
	}
}
