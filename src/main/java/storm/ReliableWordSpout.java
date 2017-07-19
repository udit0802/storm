package storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ReliableWordSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	
	private FileReader fileReader;
	
	private BufferedReader bufferReader;
	
	private Map<Integer,String> allMessages;
	
	private List<Integer> toSend;
	
	private Map<Integer,Integer> failMsgCount;
	
	private static final int MAX_FAILS = 3;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			this.fileReader = new FileReader(conf.get("reliableFileToRead").toString());
			this.bufferReader = new BufferedReader(fileReader);
			
			this.allMessages = new HashMap<>();
			this.toSend = new ArrayList<>();
			int i = 0;
			while(bufferReader.readLine() != null){
				allMessages.put(i++, bufferReader.readLine());
				toSend.add(i);
			}
			failMsgCount = new HashMap<>();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		if(!toSend.isEmpty()){
			for(int msgId : toSend){
				String word = allMessages.get(msgId);
				collector.emit(new Values(word),msgId);
			}
			toSend.clear();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
    public void ack(Object msgId) {
		System.out.println("message with id : " + msgId + " sent successfully!!");
    }

    @Override
    public void fail(Object msgId) {
    	Integer failedId = (Integer)msgId;
    	Integer failure = 1;
    	if(failMsgCount.containsKey(failedId)){
    		failure = failMsgCount.get(msgId) + 1;
    	}
    	if(failure < MAX_FAILS){
    		failMsgCount.put(failedId, failure);
    		toSend.add(failedId);
    		System.out.println("Resending the message with id [" + msgId + "]");
    	}else{
    		System.out.println("Message with id [" + msgId + "] has been failed");
    	}
    }
}
