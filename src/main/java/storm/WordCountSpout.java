package storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCountSpout extends BaseRichSpout {
	
	private FileReader fileReader;
	
	private SpoutOutputCollector spoutOutputCollector;
	
	private BufferedReader bufferReader;
	
	private boolean completed = false;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		spoutOutputCollector = collector;
		try {
			fileReader = new FileReader(conf.get("fileToRead").toString());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		bufferReader = new BufferedReader(fileReader);
	}

	@Override
	public void nextTuple() {
		if(!completed){
			try{
				String word = bufferReader.readLine();
				if(word != null){
					word = word.trim();
					word = word.toLowerCase();
					spoutOutputCollector.emit(new Values(word));
				}else{
					completed = true;
					bufferReader.close();
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
