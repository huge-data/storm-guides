package zx.soft.storm.start.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * 词频统计
 * 
 * @author wgybzb
 *
 */
public class WordCounter extends BaseBasicBolt {

	private static final long serialVersionUID = 7440534307545138182L;

	Integer id;
	String name;
	Map<String, Integer> counters;

	/**
	 * 在Spout结束的时候打印出词频（当集群关闭的时候）
	 */
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}

	/**
	 * 初始化
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		/**
		 * 新的词语，词频为1
		 */
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
	}

}
