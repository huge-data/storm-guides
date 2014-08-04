package zx.soft.storm.php.bolt;

import java.util.Map;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class PrimeNumbersFilterBolt extends ShellBolt implements IRichBolt {

	private static final long serialVersionUID = 8611110364640711063L;

	public PrimeNumbersFilterBolt() {
		super("php", "-f", "PrimeNumbersFilterBolt.php");
	}

	public PrimeNumbersFilterBolt(String... command) {
		super(command);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
