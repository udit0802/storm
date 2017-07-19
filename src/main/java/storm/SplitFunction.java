package storm;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SplitFunction extends BaseFunction {

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentance = tuple.getString(0);
		String[] words = sentance.split(" ");
		
		for(String word:words){
			word = word.trim();
			if(!word.isEmpty()){
				collector.emit(new Values(word));
			}
		}

	}

}
