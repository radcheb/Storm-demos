package storm.blueprints;

import java.util.Map;
import java.util.UUID;

import org.apache.storm.netty.util.internal.ConcurrentHashMap;

import storm.blueprints.utils.Utils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class SentenceSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private ConcurrentHashMap<UUID, Values> pending =null;
	
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
			Values values = new Values(sentences[index%sentences.length]);
			UUID msgId = UUID.randomUUID();
			this.pending.put(msgId, values);
			this.collector.emit(values, msgId);
			index++;
			Utils.waitForMillis(1);
		}
	}
	
	/*
	 * This method called when all bolts acknowledge full success of the tuple processing
	 * @see backtype.storm.topology.base.BaseRichSpout#ack(java.lang.Object)
	 */
	public void ack(Object msgId){
		this.pending.remove(msgId);
	}
	
	/*
	 * This method called when one bolt fails in the tuple processing
	 * @see backtype.storm.topology.base.BaseRichSpout#fail(java.lang.Object)
	 */
	public void fail(Object msgId){
		this.collector.emit(this.pending.get(msgId),msgId);
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
