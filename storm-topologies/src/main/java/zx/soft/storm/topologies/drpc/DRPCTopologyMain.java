package zx.soft.storm.topologies.drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;

/**
 * DRPC示例
 * 
 * @author wgybzb
 *
 */
public class DRPCTopologyMain {

	@SuppressWarnings("rawtypes")
	public static void main(String[] args) {

		// 创建本地DRPC客户端/服务器
		LocalDRPC drpc = new LocalDRPC();

		// 创建DRPC拓扑结构
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add");
		builder.addBolt(new AdderBolt(), 2);

		Config conf = new Config();
		conf.setDebug(true);

		// 创建集群并且提交拓扑
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("drpc-adder-topology", conf, builder.createLocalTopology(drpc));

		// 测试拓扑
		String result = drpc.execute("add", "1+-1");
		checkResult(result, 0);
		result = drpc.execute("add", "1+1+5+10");

		// 完成并关闭服务
		checkResult(result, 17);
		cluster.shutdown();
		drpc.shutdown();
	}

	private static boolean checkResult(String result, int expected) {
		if (result != null && !result.equals("NULL")) {
			if (Integer.parseInt(result) == expected) {
				System.out.println("Add valid [result: " + result + "]");
				return true;
			} else {
				System.err.print("Invalid result [" + result + "]");
			}
		} else {
			System.err.println("There was an error running the drpc call");
		}
		return false;
	}

}
