package storm.blueprints;

import storm.blueprints.utils.Utils;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception{
    	SentenceSpout spout= new SentenceSpout();
    	SplitSentenceBolt splitBolt = new SplitSentenceBolt();
    	WordCountBolt countBolt = new WordCountBolt();
    	ReportBolt reportBolt = new ReportBolt();
    	
    	TopologyBuilder builder = new TopologyBuilder();
    	
    	builder.setSpout(SENTENCE_SPOUT_ID, spout,2);
    	// sentenceSpout -->  splitSentenceBolt
    	builder.setBolt(SPLIT_BOLT_ID, splitBolt,2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
    	// splitSentenceBolt --> wordCountBolt
    	builder.setBolt(COUNT_BOLT_ID, countBolt,4).shuffleGrouping(SPLIT_BOLT_ID);
    	// wordCountBolt --> reportBolt
    	builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
    	
    	Config config = new Config();
    	config.setNumWorkers(2);
    	
    	LocalCluster cluster = new LocalCluster();
    	
    	cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    	
    	Utils.waitForSeconds(10);
    	
    	cluster.killTopology(TOPOLOGY_NAME);
    	cluster.shutdown();
    }
}
