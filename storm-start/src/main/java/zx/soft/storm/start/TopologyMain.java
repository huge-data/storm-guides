package zx.soft.storm.start;

import zx.soft.storm.start.bolts.WordCounter;
import zx.soft.storm.start.bolts.WordNormalizer;
import zx.soft.storm.start.spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 主类
 * 
 * @author wgybzb
 *
 */
public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {

		// 拓扑结构定义
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));

		// 配置文件
		Config conf = new Config();
		conf.put("wordsFile", "src/main/resources/words.txt");
		conf.setDebug(false);

		// 拓扑运行
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
		// 集群运行
		//		try {
		//			StormSubmitter.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		//		} catch (AlreadyAliveException | InvalidTopologyException e) {
		//			throw new RuntimeException(e);
		//		}
	}

}
