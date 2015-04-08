package storm.blueprints;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Implements the IComponent and the IBolt interfaces
 */
public class SplitSentenceBolt extends BaseRichBolt{

	private OutputCollector collector;

	/*
	 * from the IBolt interface, called each time the bolt receive a new tuple
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple tuple) {
		
		String sentence = tuple.getStringByField("sentence");
		String [] words = sentence.split(" ");
		for(String word : words){
			this.collector.emit(new Values(word));
		}
	}

	/*
	 * from the IBolt interface, used to initialize the bolt before running
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector outputCollector) {
		
		this.collector=outputCollector;
	}
	/*
	 * declare the type of output : words
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("word"));
	}

	
}
