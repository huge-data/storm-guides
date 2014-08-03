package zx.soft.storm.spouts.banktransactions;

import java.util.Map;
import java.util.Random;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class RandomFailureBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3435628336929683954L;

	private static final Integer MAX_PERCENT_FAIL = 80;
	Random random = new Random();
	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		Integer r = random.nextInt(100);
		if (r > MAX_PERCENT_FAIL) {
			collector.ack(input);
		} else {
			collector.fail(input);
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//
	}

}
