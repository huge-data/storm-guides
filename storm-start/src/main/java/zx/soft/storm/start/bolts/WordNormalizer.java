package zx.soft.storm.start.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 对每行文本进行标准化（分词、去除空格、转换成小写）
 * 
 * @author wgybzb
 *
 */
public class WordNormalizer extends BaseBasicBolt {

	private static final long serialVersionUID = 7516460439435045984L;

	@Override
	public void cleanup() {
		//
	}

	/**
	 * 标准化处理：分词、去除空格、小写化
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
	}

	/**
	 * 显示设置输出域为"word" 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
