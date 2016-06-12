package com.reed.storm.topology.rolling.bolt;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.reed.storm.topology.rolling.RollingTopTopology;

/**
 * 解析json，提取特定字段
 * @author reed
 *
 */
public class JsonSplit extends BaseFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = -459967946211196323L;
	private static final Logger LOG = LoggerFactory.getLogger(JsonSplit.class);

	@Override
	public void execute(TridentTuple paramTridentTuple,
			TridentCollector paramTridentCollector) {
		String key1 = "projectName";
		String json = paramTridentTuple.getString(0).toString();
		try {
			JSONObject obj = JSON.parseObject(json);
			if (obj != null && obj.size() > 0 && obj.containsKey(key1)) {
				String str = (String) obj.get(key1);
				LOG.info("kafka split>>>>>>>" + str);
				paramTridentCollector.emit(new Values(str));
			}
		} catch (JSONException ex) {
			LOG.error("json parse error>>>>>>" + json);
		}
	}
}
