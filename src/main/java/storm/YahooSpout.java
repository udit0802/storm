package storm;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

public class YahooSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

	public void nextTuple() {
//			StockQuote quote = YahooFinance.get("MSFT").getQuote();
//			BigDecimal price = quote.getPrice();
//			BigDecimal prevClose = quote.getPreviousClose();
			BigDecimal price = new BigDecimal(45.0);
			BigDecimal prevClose = new BigDecimal(43.0);
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			collector.emit(new Values("MSFT", sdf.format(timestamp), price.doubleValue(), prevClose.doubleValue()));

	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "timestamp", "price", "prev_close"));
	}

}
