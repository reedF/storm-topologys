package com.reed.strom.topology.es.bolt;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class GeneralInfoBolt extends BaseWindowedBolt {
	private OutputCollector collector;
	private String pName = null;
	private String cName = null;
	private String mName = null;
	private long msgTS = 0l;
	private long ts = 0l;

    @Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(TupleWindow inputWindow) {
		List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();
        Map<String, Integer> counts = new HashMap<String, Integer>();
        Map<String, HashMap<String, HashMap<String, Integer>>> pMap = new HashMap<String, HashMap<String, HashMap<String, Integer>>>();
//        HashMap<String, HashMap<String, Integer>> classMap = new HashMap<String, HashMap<String, Integer>>();
//        HashMap<String, Integer> mMap = new HashMap<String, Integer>();
        
        for (Tuple tuple : newTuples) {
        	HashMap<String, HashMap<String, Integer>> cMap = new HashMap<String, HashMap<String, Integer>>();
            HashMap<String, Integer> mMap = new HashMap<String, Integer>();
        	pName = (String) tuple.getValueByField("projectName");
        	cName = (String) tuple.getValueByField("className");
        	mName = (String) tuple.getValueByField("methodName");
        	if (null == pMap.get(pName)) {
        		mMap.put(mName, 1);
        		cMap.put(cName, mMap);
        		pMap.put(pName, cMap);	
        	} else {
        		if (null == pMap.get(pName).get(cName)) {
        			mMap.put(mName, 1);
        			pMap.get(pName).put(cName, mMap);
        		} else {
        			if (null == pMap.get(pName).get(cName).get(mName)) {
        				pMap.get(pName).get(cName).put(mName, 1);
        			} else {
        				int sum = pMap.get(pName).get(cName).get(mName);
        				sum++;
        				pMap.get(pName).get(cName).put(mName, sum);
        			}
        		}
        	}
        }	
        
        
        msgTS = (long) tuplesInWindow.get(0).getValueByField("ts");
        ts = System.currentTimeMillis();
        ArrayList<Map<String, Object>> countList = new ArrayList<Map<String, Object>>();
        
        
        for (Map.Entry<String, HashMap<String, HashMap<String, Integer>>> pEntry : pMap.entrySet()) {
        	String pKey = pEntry.getKey();
        	for (Map.Entry<String, HashMap<String, Integer>> cEntry : pEntry.getValue().entrySet()) {
        		String cKey = cEntry.getKey();
        		for (Entry<String, Integer> mEntry : cEntry.getValue().entrySet()) {
        			String mKey = mEntry.getKey();
        			int sum = mEntry.getValue();
        			Map<String, Object> SumCounts = new HashMap<String, Object>();
        			SumCounts.put("project_name", pKey);
        			SumCounts.put("class_name", cKey);
        			SumCounts.put("method_name", mKey);
        			SumCounts.put("total_count", sum);
        			SumCounts.put("timestamp", new Date(msgTS));
        			countList.add(SumCounts);
//        			System.out.println("GeneralInfoBolt : " + SumCounts);
        		}
        	}
        }

        collector.emit(new Values(countList, new Date(ts)));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("SumCountList", "ts"));
	}

}
