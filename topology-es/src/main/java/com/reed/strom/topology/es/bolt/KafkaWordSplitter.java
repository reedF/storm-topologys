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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaWordSplitter extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory
			.getLogger(KafkaWordSplitter.class);
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private String projectName = "projectName";
	private String clazzName = "clazz";
	private String methodName = "method";
	private String timestamp = null;
	private long ts = 0l;

	private String msgID = null;
	private String pName = null;
	private String cName = null;
	private String mName = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		// jedis = new Jedis("172.28.29.151", 6379);
		// jedis.del("s_server");

	}

	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);

		JSONObject jsonObj = new JSONObject(line);

		// JSON 格式化
		try {
			ts = (long) jsonObj.get("date");
		} catch (JSONException ex) {
			System.out.println("JSONException" + ex.toString());
		}

		try {
			msgID = jsonObj.getString("id");
		} catch (JSONException ex) {
			System.out.println("JSONException" + ex.toString());
		}

		try {
			// String value = jsonObj.getString(projectName);
			// this.collector.emit("projectStreamID", new Values(value, ts,
			// msgID));
			pName = jsonObj.getString(projectName);
		} catch (JSONException ex) {
			System.out.println("JSONException" + ex.toString());
		}

		try {
			// String classValue = null;
			// classValue =
			// jsonObj.getString(clazzName).split(" ")[jsonObj.getString(clazzName).split(" ").length
			// - 1];
			// this.collector.emit("classStreamID", new Values(classValue, ts,
			// msgID));
			cName = jsonObj.getString(clazzName).split(" ")[jsonObj.getString(
					clazzName).split(" ").length - 1];
		} catch (JSONException ex) {
			System.out.println("JSONException" + ex.toString());
		}

		try {
			String methodValue = null;
			// methodValue = jsonObj.getString(methodName);
			// this.collector.emit("methodStreamID", new Values(methodValue, ts,
			// msgID));
			mName = jsonObj.getString(methodName);
		} catch (JSONException ex) {
			System.out.println("JSONException" + ex.toString());
		}

		// try {
		// String outputMsg = null;
		// outputMsg = jsonObj.getString("outputInfo");
		// JSONObject outputObj = new JSONObject(outputMsg);
		// String resultCode = outputObj.getString("returnCode");
		// this.collector.emit("resultCodeID", new Values(resultCode, ts,
		// msgID));
		// }catch(JSONException ex){
		// System.out.println("KafkaWordSplitter JSONException" +
		// ex.toString());
		// }

		// this.collector.emit("splitByNameID", new Values(pName, cName, mName,
		// ts, msgID, jsonObj));
		this.collector.emit("splitByNameID", new Values(pName, cName, mName,
				ts, msgID));
		// System.out.println("KafkaWordSplitter: pName: " + pName + " cName:" +
		// cName + " mName:" + mName);

		init();

		collector.ack(input);
		// Utils.sleep(700);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("splitByNameID", new Fields("projectName",
				"className", "methodName", "ts", "msgID"));
		// declarer.declareStream("splitByNameID", new Fields("projectName",
		// "className", "methodName", "ts", "msgID", "jsonObj"));
	}

	public void init() {
		ts = 0l;
		pName = null;
		cName = null;
		mName = null;
	}

}
