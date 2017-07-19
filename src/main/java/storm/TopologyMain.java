package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		//Build the topology
//		TopologyBuilder topologyBuilder = new TopologyBuilder();
//		topologyBuilder.setSpout("Yahoo-Finance-Spout", new YahooSpout());
//		topologyBuilder.setBolt("Yahoo-Finance-Bolt", new YahooBolt(), 3).shuffleGrouping("Yahoo-Finance-Spout");
//		StormTopology topology = topologyBuilder.createTopology();
		
		//configure the topology
//		Config config = new Config();
//		config.setDebug(true);
//		config.put("fileToWrite", "/Users/b0096703/Desktop/output.txt");
		
		//Submit topology to cluster
//		LocalCluster cluster = new LocalCluster();
//		try {
//			cluster.submitTopology("Stock-Tracker-Topology", config, topology);
//			Thread.sleep(10000);
//		}finally {
//			cluster.shutdown();
//		}
		
		//Submit the topology to remote cluster
//		try {
//			StormSubmitter.submitTopology("Stock-Tracker-Topology-Remote-Parallel", config, topology);
//		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
//			e.printStackTrace();
//		}
		
		
		
		
		
		
		
		
		
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("word-reader", new WordCountSpout());
		//shuffle grouping by default
//		builder.setBolt("word-counter", new WordCountBolt(),2).shuffleGrouping("word-reader");
		
		//Field Grouping based on some criteria
//		builder.setBolt("word-counter", new WordCountBolt(),2).fieldsGrouping("word-reader", new Fields("word"));
		
		//Move all the tuples to all the bolts
//		builder.setBolt("word-counter", new WordCountBolt(),2).allGrouping("word-reader");
		
		//Move tuples based on some customization
//		builder.setBolt("word-counter", new WordCountBolt(),2).customGrouping("word-reader", new MyStreamGrouping());
//		
//		Config config = new Config();
//		config.put("fileToRead", "/Users/b0096703/Desktop/input.txt");
//		config.put("dirToWrite", "/Users/b0096703/Desktop/");
//		config.setDebug(true);
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("Word-Counter-Topology", config, builder.createTopology());
//		System.out.println("Thread going to sleep!!!");
//		Thread.sleep(10000);
//		cluster.shutdown();
		
		
		
		
		
		
		
		
		
		
		
		
//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("reliable-spout", new ReliableWordSpout());
//		builder.setBolt("reliable-fail-bolt", new RandomFailureBolt()).shuffleGrouping("reliable-spout");
//		
//		Config config = new Config();
//		config.put("reliableFileToRead", "/Users/b0096703/Desktop/input.txt");
//		config.setDebug(true);
//		
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("relaible-word-topology", config, builder.createTopology());
//		System.out.println("Thread going to sleep!!!");
//		Thread.sleep(10000);
//		cluster.shutdown();
		
		
		
		
		
		
		
		
		
		
//		Implement the trident Topology(simple)
//		TridentTopology topology = new TridentTopology();
//		topology.newStream("lines", new WordCountSpout()).each(new Fields("word"), new SplitFunction(), new Fields("word_split")).
//		each(new Fields("word_split"), new Debug());
//		
//		Config config = new Config();
//		config.setDebug(true);
//		config.put("fileToRead", "/Users/b0096703/Desktop/sample.txt");
//		
//		if(args.length != 0 && args[0].equals("remote")){
//				StormSubmitter.submitTopology("Trident-Topology-remote", config, topology.build());
//		}else{
//			LocalCluster cluster = new LocalCluster();
//			cluster.submitTopology("Trident-Topology-local", config, topology.build());
//			Thread.sleep(10000);
//			cluster.shutdown();
//		}
		//Generates all the words with their count
//		TridentTopology topology = new TridentTopology();
//		topology.newStream("lines", new WordCountSpout()).each(new Fields("word"), new SplitFunction(), new Fields("word_split")).groupBy(new Fields("word_split"))
//		.aggregate(new Count(), new Fields("count")).each(new Fields("word_split","count"),new Debug());
//		
//		Config config = new Config();
//		config.setDebug(true);
//		config.put("fileToRead", "/Users/b0096703/Desktop/sample.txt");
//		
//		if(args.length != 0 && args[0].equals("remote")){
//				StormSubmitter.submitTopology("Trident-Topology-remote", config, topology.build());
//		}else{
//			LocalCluster cluster = new LocalCluster();
//			cluster.submitTopology("Trident-Topology-local", config, topology.build());
//			Thread.sleep(10000);
//			cluster.shutdown();
//		}
		
		
//		LocalDRPC drpc = new LocalDRPC();
//		TridentTopology topology = new TridentTopology();
//		topology.newDRPCStream("split",drpc).each(new Fields("args"), new SplitFunction(),new Fields("word_split")).groupBy(new Fields("word_split"))
//		.aggregate(new Count(), new Fields("count"));
//		
//		Config config = new Config();
//		config.setDebug(true);
//		
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("Trident-Topology-local-drpc", config, topology.build());
//		
//		for(String word : new String[]{"a very very short book", "this is a very very long book"}){
//			System.out.println("Result for :" + word + " : " + drpc.execute("split", word));
//		}
//		cluster.shutdown();
		
		LocalDRPC drpc = new LocalDRPC();
		TridentTopology topology = new TridentTopology();
		TridentState wordCounts = topology.newStream("lines",new WordCountSpout()).each(new Fields("word"), new SplitFunction(),new Fields("word_split")).groupBy(new Fields("word_split"))
		.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
		
		topology.newDRPCStream("count",drpc).stateQuery(wordCounts, new Fields("args"), new MapGet(), new Fields("count"));
		
		Config config = new Config();
		config.setDebug(true);
		config.put("fileToRead", "/Users/b0096703/Desktop/sample.txt");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident-topology-query", config, topology.build());
		Thread.sleep(10000);
		for(String word : new String[]{"is", "very"}){
			System.out.println("Result for the word :" + word + " is : " + drpc.execute("count", word));
		}
		cluster.shutdown();
	}

}
