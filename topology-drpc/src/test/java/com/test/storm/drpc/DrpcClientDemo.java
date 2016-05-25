package com.test.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;

import com.reed.storm.topology.drpc.DrpcTopology;

/**
 * Drpc-client，测试drpc 服务端
 * @author reed
 *
 */
public class DrpcClientDemo {

	public static void main(String[] args) throws DRPCExecutionException,
			AuthorizationException, TException, InterruptedException {
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("storm.thrift.transport",
				"org.apache.storm.security.auth.SimpleTransportPlugin");
		conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
		conf.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576);
		DRPCClient client = new DRPCClient(conf, "172.28.19.78", 3772);
		String str = "kafka";
		for (int i = 0; i < 100; i++) {
		String r = client.execute(DrpcTopology.functionName, str);
		System.out.println(">>>>>>>>>>>>>>>>Results:" + r);
		 Thread.sleep((long) 5 * 1000);
		 }
	}

}
