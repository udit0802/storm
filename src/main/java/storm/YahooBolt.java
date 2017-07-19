package storm;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class YahooBolt extends BaseBasicBolt {
	
//	private PrintWriter printWriter;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context){
//		String fileName =  stormConf.get("fileToWrite").toString();
//		try {
//			printWriter = new PrintWriter(fileName,"UTF-8");
//		} catch (FileNotFoundException|UnsupportedEncodingException e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String symbol = input.getValue(0).toString();
		String timeStamp = input.getString(1);
		
		Double price = (Double)input.getValueByField("price");
		Double prevClose = input.getDoubleByField("prev_close");
		
		Boolean gain = true;
		 if(price <= prevClose){
			 gain = false;
		 }
		 collector.emit(new Values(symbol, timeStamp, price, gain));
//		 printWriter.println(symbol + "," + timeStamp + "," + price + "," + gain);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "timestamp", "price", "gain"));
	}

	@Override
    public void cleanup() {
//		printWriter.close();
    }
}
