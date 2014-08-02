package zx.soft.storm.topologies.countword.grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

/**
 * 自定义词频统计的Grouping，使得以相同字母开头的单词输入到同一个bolt。
 * 
 * @author wgybzb
 *
 */
public class ModuleGrouping implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = 1733815100670976138L;

	int numTasks = 0;

	@Override
	public void prepare(TopologyContext context, Fields outFields, List<Integer> targetTasks) {
		numTasks = targetTasks.size();

	}

	@Override
	public List<Integer> chooseTasks(List<Object> values) {
		List<Integer> boltIds = new ArrayList<>();
		if (values.size() > 0) {
			String str = values.get(0).toString();
			if (str.isEmpty()) {
				boltIds.add(0);
			} else {
				boltIds.add(str.charAt(0) % numTasks);
			}
		}
		return boltIds;
	}

}
