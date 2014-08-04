package zx.soft.storm.php;

import zx.soft.storm.php.bolt.PrimeNumbersFilterBolt;
import zx.soft.storm.php.spout.NumberGeneratorSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopologyMain {

	/**
	 * 主函数
	 */
	public static void main(String[] args) throws InterruptedException {

		// 拓扑结构定义
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("numbers-generator", new NumberGeneratorSpout(1, 10000));
		builder.setBolt("prime-numbers-filter", new PrimeNumbersFilterBolt()).shuffleGrouping("numbers-generator");

		// 配置文件
		Config conf = new Config();
		conf.setDebug(false);

		// 拓扑运行
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
	}
}
