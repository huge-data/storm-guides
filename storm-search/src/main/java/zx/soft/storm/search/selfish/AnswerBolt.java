package zx.soft.storm.search.selfish;

import zx.soft.storm.search.storm.AbstractAnswerBolt;
import backtype.storm.tuple.Tuple;

public class AnswerBolt extends AbstractAnswerBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input) {
		String origin = input.getString(0);
		String requestId = input.getString(1);
		log.debug("Received tuple: origin:" + origin + " requestId:" + requestId);
		sendBack(origin, requestId, "I'm not a bank!");
	}

	@Override
	protected int getDestinationPort() {
		return 8082;
	}

}
