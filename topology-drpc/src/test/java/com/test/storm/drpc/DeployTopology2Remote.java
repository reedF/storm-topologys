package com.test.storm.drpc;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.thrift.TException;

import com.reed.storm.topology.drpc.DrpcTopology;
import com.reed.storm.utils.StormRunner;

public class DeployTopology2Remote {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException, TException,
			InterruptedException {
		DrpcTopology drpcTopology = new DrpcTopology();
		String jar = System.getProperty("user.dir")
				+ "/target/topology-drpc-1.0-SNAPSHOT.jar";
		// upload and run
		//注：此方式提交DrpcTopology时，远程topolgy运行在loacl模式，连接的zk为localhost,报异常，无法获取return值
		//可通过在远程使用storm jar 提交即可正常使用,原因待查(怀疑nimbus.getClient().submitTopology提交时是否有默认参数)
		StormRunner.deployJar2Remote("172.28.19.91", 6627, jar, drpcTopology
				.getClass().getSimpleName(), drpcTopology.wordCount().build());
	}
}
