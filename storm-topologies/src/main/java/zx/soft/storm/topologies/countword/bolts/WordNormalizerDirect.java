package zx.soft.storm.topologies.countword.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 通过指定bolt的方式标准化。
 * 注：和自定义Grouping效果一样，这里是emit时指定对应的bolt。
 * 
 * @author wgybzb
 *
 */
public class WordNormalizerDirect extends BaseRichBolt {

	private static final long serialVersionUID = 4288891435174590332L;

	private OutputCollector collector;
	int numCounterTasks = 0;

	@Override
	public void cleanup() {
		//
	}

	@Override
	public void execute(Tuple input) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emitDirect(getWordCountIndex(word), new Values(word));
			}
		}
		// Acknowledge the tuple
		collector.ack(input);
	}

	/**
	 * 获取单词对应的bolt号
	 */
	public Integer getWordCountIndex(String word) {
		word = word.trim().toLowerCase();
		if (word.isEmpty()) {
			return 0;
		} else {
			return word.charAt(0) % numCounterTasks;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.numCounterTasks = context.getComponentTasks("word-counter").size();
	}

	/**
	 * 设置输出域为"word" 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
