package com.reed.storm.topology.rolling.redis;

import java.util.List;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Lists;
import com.reed.storm.topology.rolling.RollingTopTopology;

public class RollingRedisLookupMapper implements RedisLookupMapper {
	private RedisDataTypeDescription description;
	public static final String hashKey = "rolling_state";

	public RollingRedisLookupMapper() {
		description = new RedisDataTypeDescription(
				RedisDataTypeDescription.RedisDataType.HASH, hashKey);
	}

	@Override
	public List<Values> toTuple(ITuple input, Object value) {
		String member = getKeyFromTuple(input);
		List<Values> values = Lists.newArrayList();
		values.add(new Values(member, value));
		return values;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(RollingTopTopology.projectName, "count"));
	}

	@Override
	public RedisDataTypeDescription getDataTypeDescription() {
		return description;
	}

	@Override
	public String getKeyFromTuple(ITuple tuple) {
		return tuple.getStringByField("arg");
	}

	@Override
	public String getValueFromTuple(ITuple tuple) {
		return null;
	}
}