package com.reed.storm.topology.rolling;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateQuerier;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStateFactory;
import org.apache.storm.trident.windowing.WindowsStateUpdater;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reed.storm.topology.rolling.bolt.CountMapAsAggregator;
import com.reed.storm.topology.rolling.bolt.FunctionNullToZero;
import com.reed.storm.topology.rolling.bolt.JsonSplit;
import com.reed.storm.topology.rolling.bolt.NowCount;
import com.reed.storm.topology.rolling.bolt.WindowMapGet;
import com.reed.storm.topology.rolling.redis.RollingRedisLookupMapper;
import com.reed.storm.topology.rolling.redis.RollingRedisStoreMapper;
import com.reed.storm.utils.StormRunner;

/**
 * 滑动窗口统计与排序TOP N topology，使用DRPC模式提供实时统计数据，给DRPC-client实时调用查询
 * @author reed
 *
 */
public class RollingTopTopology {
	private static final Logger LOG = LoggerFactory
			.getLogger(RollingTopTopology.class);

	private static final String topologyName = RollingTopTopology.class
			.getSimpleName();

	private static WindowsStoreFactory windowStoreFactory = new InMemoryWindowsStoreFactory();
	private static WindowsStateFactory windowStateFactory = new WindowsStateFactory();
	private static WindowsStateUpdater stateUpdater = new WindowsStateUpdater(
			windowStoreFactory);

	public static final String functionName = "drpc-rolling";
	public static final String functionNameByArg = "drpc-rolling-arg";
	public static final String txId = RollingTopTopology.class.getSimpleName();
	public static final String projectName = "projectName";

	private static final String zkUrl = "172.28.20.103:2181,172.28.20.104:2181,172.28.20.105:2181";
	private static final String topic = "test";
	private static final String redisHost = "172.28.29.151";

	public static OpaqueTridentKafkaSpout createKafkaSpout() {
		String clientId = RollingTopTopology.class.getSimpleName();
		ZkHosts hosts = new ZkHosts(zkUrl);
		TridentKafkaConfig config = new TridentKafkaConfig(hosts, topic,
				clientId);
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		// Consume new data from the topic
		config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return new OpaqueTridentKafkaSpout(config);
	}

	public static RedisState.Factory createRedisFactory(String redisHost,
			int redisPort) {
		JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
				.setHost(redisHost).setPort(redisPort).build();
		return new RedisState.Factory(poolConfig);
	}

	@SuppressWarnings("serial")
	public static Stream addWindow2Topology(TridentTopology tridentTopology) {
		return tridentTopology
				.newStream(txId, createKafkaSpout())
				.parallelismHint(2)
				.each(new Fields("str"), new JsonSplit(),
						new Fields(projectName))
				// .window(TumblingDurationWindow.of(new
				// BaseWindowedBolt.Duration(
				// 1, TimeUnit.SECONDS)),
				// new InMemoryWindowsStoreFactory(),
				// new Fields(projectName), new CountAsAggregator(),
				// new Fields(projectName))
				.project(new Fields(projectName))
				.groupBy(new Fields(projectName))
				.toStream()
				.tumblingWindow(
						new BaseWindowedBolt.Duration(1, TimeUnit.SECONDS),
						windowStoreFactory, new Fields(projectName),
						new CountMapAsAggregator(),
						new Fields(projectName, "num")).peek(new Consumer() {
					@Override
					public void accept(TridentTuple input) {
						LOG.info("Received tuple: [{},{}]",
								input.getValueByField(projectName),
								input.getValueByField("num"));
					}
				});
	}

	public static TridentState addMemaryState(Stream steam) {

		return steam.groupBy(new Fields(projectName)).persistentAggregate(
				new MemoryMapState.Factory(), new Fields("num"),
				new NowCount(), new Fields("count"));
		// .partitionPersist(windowStateFactory, new Fields(projectName),
		// stateUpdater, new Fields("count"));
	}

	public static TridentState addRedisState(Stream steam) {

		return steam.partitionPersist(createRedisFactory(redisHost, 6379),
				new Fields(projectName, "num"), new RedisStateUpdater(
						new RollingRedisStoreMapper()).withExpire(300),
				new Fields("count"));
	}

	public static void drpcResponseInMemary(TridentTopology tridentTopology,
			LocalDRPC drpc) {
		TridentState state = addMemaryState(addWindow2Topology(tridentTopology));
		// state.newValuesStream().peek(new Consumer() {
		// @Override
		// public void accept(TridentTuple input) {
		// LOG.info("state tuple: [{}]", input);
		// }
		// });
		tridentTopology
				.newDRPCStream(functionName, drpc)
				// 切分DRPC请求参数
				.each(new Fields("args"), new Split(), new Fields(projectName))
				.groupBy(new Fields(projectName))
				.stateQuery(state, new Fields(projectName), new MapGet(),
						new Fields("count"))
				.each(new Fields("count"), new FunctionNullToZero(),
						new Fields("result")).peek(new Consumer() {
					@Override
					public void accept(TridentTuple input) {
						// LOG.info("DRPC tuple: [{}]", input);
					}
				}).project(new Fields(projectName, "result"));
	}

	public static void drpcResponseInRedis(TridentTopology tridentTopology,
			LocalDRPC drpc) {
		TridentState state = addRedisState(addWindow2Topology(tridentTopology));
		tridentTopology
				.newDRPCStream(functionName, drpc)
				// 切分DRPC请求参数
				.each(new Fields("args"), new Split(), new Fields("arg"))
				.groupBy(new Fields("arg"))
				.stateQuery(state, new Fields("arg"),
						new RedisStateQuerier(new RollingRedisLookupMapper()),
						new Fields(projectName,"count","num")).peek(new Consumer() {
					@Override
					public void accept(TridentTuple input) {
						//LOG.info("DRPC tuple: [{}]", input);
					}
				}).project(new Fields(projectName, "count"));
	}

	/**
	 * DRPC简单使用，统计kafka topic内wordcount
	 * @return
	 */
	public static TridentTopology wordCount(LocalDRPC drpc) {
		TridentTopology tridentTopology = new TridentTopology();

		// drpcResponseInMemary(tridentTopology, drpc);
		drpcResponseInRedis(tridentTopology, drpc);

		return tridentTopology;
	}

	/**
	 * 校验打印启动参数
	 * @param args
	 */
	public static void checkParameter(String[] args) {
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

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException,
			InterruptedException {

		Config conf = new Config();
		conf.setNumWorkers(2);
		checkParameter(args);
		// remote
		if (args == null || args.length == 0) {
			StormRunner.runTopologyRemotely(wordCount(null).build(),
					topologyName, conf);
		} else if ("local".equals(args[0])) {
			// local
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, wordCount(drpc).build());
			String str = "test openstacklog test2";
			for (int i = 0; i < 50; i++) {
				String r = drpc.execute(functionName, str);
				LOG.info(">>>>>>>>>>>>>>>>Results:" + r);
				Thread.sleep(2000);
			}
			cluster.shutdown();
			drpc.shutdown();
			System.exit(1);
		} else {
			LOG.error(">>>>>>>>>ERROR:paramters for main method are wrong!");
		}
	}
}
