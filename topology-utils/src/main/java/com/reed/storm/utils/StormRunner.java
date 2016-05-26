package com.reed.storm.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

/**
 * storm runner for remote or local to run a topology
 * @author reed
 *
 */
public final class StormRunner {

	private static final int MILLIS_IN_SEC = 1000;

	private StormRunner() {
	}

	/**
	 * 以本地模式运行topology，用于本地调试
	 * @param topology
	 * @param topologyName
	 * @param conf
	 * @param runtimeInSeconds
	 * @throws InterruptedException
	 */
	public static void runTopologyLocally(StormTopology topology,
			String topologyName, Config conf, int runtimeInSeconds)
			throws InterruptedException {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, topology);
		Thread.sleep((long) runtimeInSeconds * MILLIS_IN_SEC);
		cluster.killTopology(topologyName);
		cluster.shutdown();
		System.exit(1);
	}

	/**
	 * 以远程模式运行topology
	 * @param topology
	 * @param topologyName
	 * @param conf
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws AuthorizationException
	 */
	public static void runTopologyRemotely(StormTopology topology,
			String topologyName, Config conf) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException {
		StormSubmitter.submitTopology(topologyName, conf, topology);
	}

	/**
	 * 上传jar并在远程运行topology,注：此方法会根据topologyName先kill掉原有topology
	 * @param nimbusIp
	 * @param nimbusPort
	 * @param jarPath
	 * @param topologyName
	 * @param topology
	 * @throws AlreadyAliveException
	 * @throws InvalidTopologyException
	 * @throws AuthorizationException
	 * @throws TException
	 * @throws InterruptedException 
	 */
	@SuppressWarnings("unchecked")
	public static void deployJar2Remote(String nimbusIp, int nimbusPort,
			String[] zkIps, int zkPort, String jarPath, String topologyName,
			StormTopology topology) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException, TException,
			InterruptedException {
		// 读取本地 Storm 配置文件
		Map storm_conf = Utils.readStormConfig();
		// 设定为远程storm配置
		storm_conf.put(Config.NIMBUS_SEEDS,
				Arrays.asList(new String[] { nimbusIp }));
		storm_conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(zkIps));
		storm_conf.put(Config.NIMBUS_THRIFT_PORT, nimbusPort);
		storm_conf.put(Config.STORM_ZOOKEEPER_PORT, zkPort);
		Client client = NimbusClient.getConfiguredClient(storm_conf)
				.getClient();
		NimbusClient nimbus = new NimbusClient(storm_conf, nimbusIp, nimbusPort);
		// upload topology jar to Cluster using StormSubmitter
		String uploadedJarLocation = StormSubmitter.submitJar(storm_conf,
				jarPath);
		String jsonConf = JSONValue.toJSONString(storm_conf);
		List<TopologySummary> topologyList = nimbus.getClient()
				.getClusterInfo().get_topologies();
		if (topologyList != null) {
			for (TopologySummary t : topologyList) {
				if (t != null && topologyName.equals(t.get_name())) {
					nimbus.getClient().killTopology(topologyName);
					Thread.sleep((long) 20 * MILLIS_IN_SEC);
					break;
				}
			}
		}
		nimbus.getClient().submitTopology(topologyName, uploadedJarLocation,
				jsonConf, topology);
	}
}