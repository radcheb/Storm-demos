package storm.blueprints;

import java.util.Map;

import storm.blueprints.utils.Utils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class SentenceSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private String[] sentences = { "my dog has fleas", "i like cold beverages",
			"the dog ate my homework", "don't have a cow man",
			"i don't think i like fleas" };

	private int index = 0;
	
	/*
	 * Called to request next tuple in the collector
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		
		if(index < sentences.length){
			this.collector.emit(new Values(sentences[index%sentences.length]));
			index++;
			Utils.waitForMillis(1);
		}
	}
	/*
	 * from the ISpout interface and is called on the spout initialization
	 * map : contain storm configuration
	 * context: contain topology components
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector outputCollector) {
		
		this.collector = outputCollector;
	}
	/*
	 * from the IComponent interface; declare the type out the output streams
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
