package com.test.spoutkafka;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.*;


import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


public class KafkaSpoutTestTopology {
	public static final Logger LOG=LoggerFactory.getLogger(KafkaSpoutTestTopology.class);
	
	public static String kafka_zk=null;
	
	
	
	
	
	public static BrokerHosts brokerHosts;
	public static String spout_id;
	public static String topic;
	
	
	private static  class PrintBolt extends BaseBasicBolt
	{

		public void execute(Tuple tuple, BasicOutputCollector arg1) {
			System.out.println("***************88"+tuple);
		}
		public void declareOutputFields(OutputFieldsDeclarer arg0) {	
		}	
	}
	
	public  void prepare()
	{
		Properties props = new Properties();
		try{
		props.load(new FileInputStream("D:\\software\\eclipsemars\\storm\\stormexample\\src\\main\\resources\\config.properties"));
		kafka_zk=props.getProperty("kafka_zk");
		
		spout_id=props.getProperty("spout_id");
		topic=props.getProperty("topic");
		/*
		 * Here, brokerZkStr can be localhost:9092 and brokerZkPath is the root directory 
		 * under which all the topic
		 * and partition information is stored. The default value of brokerZkPath is /brokers.
		 * */
		brokerHosts = new ZkHosts(kafka_zk);
		
		
		}catch(IOException e)
		{
			e.printStackTrace();
		}
	}
	public StormTopology buildTopology( )
	{
		this.prepare();

		//brokerHosts = new ZkHosts(kafka_zk);
		// 这里的 第三个参数和第四个参数都不是固定的，可以随便写，但最后是 / 开头，没有测试。
		// brokerHosts 指zookeeper的地址IP:port, topic 是固定的
		SpoutConfig spout_conf = new SpoutConfig(brokerHosts,topic,"/"+"test",spout_id);
		spout_conf.scheme=new SchemeAsMultiScheme(new StringScheme());

		
		KafkaSpout kafkaspout = new KafkaSpout(spout_conf);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("original_data",   kafkaspout, 2);
		builder.setBolt("bolt", new PrintBolt(),2).shuffleGrouping("original_data");

		return builder.createTopology();
	}
	
	
	
	public static void main(String[] args) throws Exception {
		KafkaSpoutTestTopology  kafkaSpoutTestTopology = new KafkaSpoutTestTopology();
		StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
		Config config = new Config();
		
		if (args != null && args.length > 1) {
            String name = args[1];
//            config.setMaxSpoutPending(Integer.parseInt(max_spout_pending));
//            config.setNumWorkers(Integer.parseInt(num_workers));
//            config.setNumAckers(Integer.parseInt(num_ackers));
//            config.setDebug(Boolean.parseBoolean(debug));
//    		// 一些bolt可能用到的配置
//            config.put("mongo_addr", mongo_client_addr_str);
//            config.put("redis_addr", redis_client_addr_str);
            StormSubmitter.submitTopology(name, config, stormTopology);
		} else {
			config.setNumWorkers(2);
			config.setMaxTaskParallelism(2);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka", config, stormTopology);
		}
	}

}
