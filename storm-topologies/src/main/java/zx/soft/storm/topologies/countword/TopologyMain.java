package zx.soft.storm.topologies.countword;

import zx.soft.storm.topologies.countword.bolts.WordCounter;
import zx.soft.storm.topologies.countword.bolts.WordNormalizer;
import zx.soft.storm.topologies.countword.spouts.SignalsSpout;
import zx.soft.storm.topologies.countword.spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setSpout("signals-spout", new SignalsSpout());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		// 自定义Grouping
		//		builder.setBolt("word-normalizer", new WordNormalizer()).customGrouping("word-reader", new ModuleGrouping());

		builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"))
				.allGrouping("signals-spout", "signals");
		// 通过直接方式来gourping
		//		builder.setBolt("word-counter", new WordNormalizerDirect(), 2).directGrouping("word-normalizer");

		// Configuration
		Config conf = new Config();
		conf.put("wordsFile", args[0]);
		conf.setDebug(true);

		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Count-Word-Toplogy-With-Refresh-Cache", conf, builder.createTopology());
		Thread.sleep(5000);
		cluster.shutdown();
	}
}
