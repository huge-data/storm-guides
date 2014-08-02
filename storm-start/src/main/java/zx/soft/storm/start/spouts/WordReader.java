package zx.soft.storm.start.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 读取每行数据
 * 
 * @author wgybzb
 *
 */
public class WordReader extends BaseRichSpout {

	private static final long serialVersionUID = -7579781317450034331L;

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;

	@Override
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	@Override
	public void close() {
		//
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	/**
	 * 将输入文件按行输出 
	 */
	@Override
	public void nextTuple() {
		/**
		 * 该函数一直会被调用，如果已经读取过文件的话，将会等待并且返回
		 */
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//
			}
			return;
		}
		String str;
		// 打开reader
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			// 读取所有行的数据
			while ((str = reader.readLine()) != null) {
				/**
				 * 每行作为输出
				 */
				this.collector.emit(new Values(str), str);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	/**
	 * 创建文件，并获取collector对象
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
		}
		this.collector = collector;
	}

	/**
	 * 显示设置输出域为"word"
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
