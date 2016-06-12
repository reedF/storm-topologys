package com.test.storm.rolling;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.thrift.TException;

import com.reed.storm.topology.rolling.RollingTopTopology;
import com.reed.storm.utils.StormRunner;

public class DeployTopology2Remote {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException, TException,
			InterruptedException {
		String[] zkIps = new String[] { "172.28.20.103", "172.28.20.104",
				"172.28.20.105" };
		RollingTopTopology topology = new RollingTopTopology();
		String jar = System.getProperty("user.dir")
				+ "/target/topology-rollingTop-1.0-SNAPSHOT.jar";
		StormRunner.deployJar2Remote("172.28.19.91", 6627, zkIps, 2181, jar,
				topology.getClass().getSimpleName(), RollingTopTopology
						.wordCount(null).build());
	}
}
