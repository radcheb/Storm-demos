package storm.blueprints;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt{

	private HashMap<String, Long> counts =null;
	
	public void execute(Tuple tuple) {
		
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		Long old_count = counts.get(word);
		if(old_count == null){
			counts.put(word, count);
		}else{
			counts.put(word, count+old_count);			
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.counts=new HashMap<String, Long>();
	}

	/*
	 * Empty because this is a terminal bolt
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		 // this bolt does not emit anything
	}

	/*
	 * This function called when the topology is killed or stopped, we will use it to report final results
	 * @see backtype.storm.topology.base.BaseRichBolt#cleanup()
	 */
	public void cleanup(){
		System.out.println("--- Final Counts ---");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		Collections.sort(keys);
		for(String key: keys){
			System.out.println(key+" : "+this.counts.get(key));
		}
		System.out.println("-------------------");
	}

}
