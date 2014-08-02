package zx.soft.storm.topologies.countword.bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseRichBolt {

	private static final long serialVersionUID = 3138259118887046545L;

	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;

	@Override
	public void cleanup() {
		System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}

	@Override
	public void execute(Tuple input) {
		String str = null;
		try {
			str = input.getStringByField("word");
		} catch (IllegalArgumentException e) {
			//
		}

		if (str != null) {
			if (!counters.containsKey(str)) {
				counters.put(str, 1);
			} else {
				Integer c = counters.get(str) + 1;
				counters.put(str, c);
			}
		} else {
			if (input.getSourceStreamId().equals("signals")) {
				str = input.getStringByField("action");
				if ("refreshCache".equals(str))
					counters.clear();
			}
		}
		// 设置tuple已收到通知
		collector.ack(input);
	}

	/**
	 * On create 
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//
	}

}
