package zx.soft.storm.php.spout;

import java.util.Map;

import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class NumberGeneratorSpout extends ShellSpout implements IRichSpout {

	private static final long serialVersionUID = -5982390263776675116L;

	public NumberGeneratorSpout(Integer from, Integer to) {
		super("php", "-f", "NumberGeneratorSpout.php", from.toString(), to.toString());
	}

	public NumberGeneratorSpout(String... command) {
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
