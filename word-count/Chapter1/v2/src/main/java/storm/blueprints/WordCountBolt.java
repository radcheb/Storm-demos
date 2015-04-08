package storm.blueprints;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt{

	private OutputCollector collector=null;
	private HashMap<String, Long> counts = null;

	public void execute(Tuple tuple) {
		
		String word = tuple.getStringByField("word");
		Long count = this.counts.get(word);
		if(count == null){
			count = 0L;
		}
		count ++;
		this.counts.put(word, count);
		this.collector.emit(tuple ,new Values(word,count));
	}
	/*
	 * The initialization method,  all non serializable object could be initialized here
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	public void prepare(Map conf, TopologyContext topo, OutputCollector collector) {
		
		this.collector=collector;
		this.counts=new HashMap<String, Long>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("word","count"));
	}

}
