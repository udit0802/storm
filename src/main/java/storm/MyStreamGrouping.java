package storm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class MyStreamGrouping implements CustomStreamGrouping, Serializable {
	
	private List<Integer> targetTasks;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		this.targetTasks = targetTasks;
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<>();
		String word = values.get(0).toString();
		word.toLowerCase();
		if(word.startsWith("a")){
			boltIds.add(targetTasks.get(0));
		}else{
			boltIds.add(targetTasks.get(1));
		}
		return boltIds;
	}

}
