package jvm.storm.starter.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -7268315948007368121L;

	SpoutOutputCollector _collector;
	Random _rand;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		String[] sentences = new String[] { "the cow jumped over the moon", "an apple a day keeps the doctor away",
				"four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
		String sentence = sentences[_rand.nextInt(sentences.length)];
		_collector.emit(new Values(sentence));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}