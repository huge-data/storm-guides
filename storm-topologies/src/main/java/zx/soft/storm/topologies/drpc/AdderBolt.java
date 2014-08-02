package zx.soft.storm.topologies.drpc;

import java.security.InvalidParameterException;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("hiding")
public class AdderBolt<String> extends BaseBasicBolt {

	private static final long serialVersionUID = -825808556651614750L;

	private static final Object NULL = "NULL";
	@SuppressWarnings("unused")
	private OutputCollector collector;

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// 解析表达式
		String[] numbers = (String[]) input.getString(1).split("\\+");
		Integer added = 0;
		try {
			if (numbers.length < 2) {
				throw new InvalidParameterException("Should be at least 2 numbers");
			}
			for (String num : numbers) {
				// 添加每个元素
				added += Integer.parseInt((java.lang.String) num);
			}
		} catch (Exception e) {
			// 错误是输出null
			collector.emit(new Values(input.getValue(0), NULL));
		}
		// 输出结果
		collector.emit(new Values(input.getValue(0), added));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result"));
	}

}
