package storm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class WordCountBolt extends BaseBasicBolt {
	
	private Map<String,Integer> counters;
	
	private Integer Id;
	
	private String name;
	
	private String fileName;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context){
		counters = new HashMap<String,Integer>();
		Id = context.getThisTaskId();
		name = context.getThisComponentId();
		fileName = stormConf.get("dirToWrite").toString() + "output"+"-" + name + Id + ".txt";
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = (String)input.getValueByField("word");
		if(!counters.containsKey(word)){
			counters.put(word, 1);
		}else{
			Integer count = counters.get(word);
			counters.put(word, count + 1);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	
	@Override
    public void cleanup() {
		try {
			PrintWriter writer = new PrintWriter(fileName,"UTF-8");
			for(Map.Entry<String, Integer> entry : counters.entrySet()){
				writer.println(entry.getKey() + " :" + entry.getValue());
			}
			writer.close();
		} catch (FileNotFoundException|UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
