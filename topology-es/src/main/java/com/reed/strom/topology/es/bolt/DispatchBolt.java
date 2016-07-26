package com.reed.strom.topology.es.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONException;
import org.json.JSONObject;

public class DispatchBolt extends BaseRichBolt {
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String pName = (String) input.getValueByField("projectName");
		String cName = (String) input.getValueByField("className");
		String mName = (String) input.getValueByField("methodName");
		long msgTS = (long) input.getValueByField("ts");
//		JSONObject jsonObj = (JSONObject) input.getValueByField("jsonObj");
//		JSONObject outputObj = new JSONObject();
		
//		try {
//			String outputMsg = null;
//			outputMsg = jsonObj.getString("outputInfo");
//			outputObj = new JSONObject(outputMsg);
//			
//			String metaMsg = outputObj.getString("meta");
//			JSONObject metaObj = new JSONObject(metaMsg);
//			
//			String codeMsg = metaObj.getString("code");
//			
//			if (0 == Integer.parseInt(codeMsg)) {
//				this.collector.emit("StatusCodeStreamID", new Values(codeMsg, pName, cName, mName, msgTS, jsonObj));
//			}
//		} catch (JSONException e) {
////			System.out.println("DispatchBolt JSONException" + e.toString());
//		}
		
//		this.collector.emit("GeneralStreamID", new Values(pName, cName, mName, msgTS, jsonObj));
		this.collector.emit("GeneralStreamID", new Values(pName, cName, mName, msgTS));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declareStream("GeneralStreamID", new Fields("projectName", "className", "methodName", "ts", "jsonObj"));
		declarer.declareStream("GeneralStreamID", new Fields("projectName", "className", "methodName", "ts"));
//		declarer.declareStream("StatusCodeStreamID", new Fields("returnCode", "projectName", "className", "methodName", "ts", "jsonObj"));
	}

}
