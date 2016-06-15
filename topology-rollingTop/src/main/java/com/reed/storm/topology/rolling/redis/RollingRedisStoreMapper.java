package com.reed.storm.topology.rolling.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

import com.reed.storm.topology.rolling.RollingTopTopology;

public class RollingRedisStoreMapper implements RedisStoreMapper {
	private RedisDataTypeDescription description;

	public RollingRedisStoreMapper() {
		description = new RedisDataTypeDescription(
				RedisDataTypeDescription.RedisDataType.HASH,
				RollingRedisLookupMapper.hashKey);
	}

	@Override
	public RedisDataTypeDescription getDataTypeDescription() {
		return description;
	}

	@Override
	public String getKeyFromTuple(ITuple tuple) {
		return tuple.getStringByField(RollingTopTopology.projectName);
	}

	@Override
	public String getValueFromTuple(ITuple tuple) {
		return String.valueOf((tuple.getLongByField("num")));
	}
}