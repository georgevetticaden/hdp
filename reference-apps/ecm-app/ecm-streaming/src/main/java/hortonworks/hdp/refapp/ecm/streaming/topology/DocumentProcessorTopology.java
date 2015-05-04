package hortonworks.hdp.refapp.ecm.streaming.topology;



import hortonworks.hdp.refapp.ecm.streaming.bolt.docstore.DocumentStoreBolt;
import hortonworks.hdp.refapp.ecm.streaming.bolt.indexstore.IndexStoreBolt;
import hortonworks.hdp.refapp.ecm.streaming.spout.kafka.DocumentScheme;

import java.util.Properties;

import org.apache.log4j.Logger;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;


public class DocumentProcessorTopology extends BaseDocumentTopology {
	
	private static final Logger LOG = Logger.getLogger(DocumentProcessorTopology.class);
	
	/**
	 * Pass in a config file to configure the Storm Topology
	 * @param configFileLocation
	 * @throws Exception
	 */
	public DocumentProcessorTopology(String configFileLocation) throws Exception {
		super(configFileLocation);			
	}
	
	/**
	 * Pass in Properties object to configure the Storm Topology
	 * @param prop
	 * @throws Exception
	 */
	public DocumentProcessorTopology(Properties prop) throws Exception {
		this.topologyConfig = prop;		
	}	
	
	/**
	 * Main Method to build and then deploy the topology
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String configFileLocation = args[0];
		DocumentProcessorTopology truckTopology = new DocumentProcessorTopology(configFileLocation);
		truckTopology.buildAndSubmit();
		
	}		
	
	/**
	 * Build up Storm Topology that can be submitted
	 * Used if you want to programatically deploy the topology
	 * @return
	 */
	public StormTopology buildTopology() {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/* Set up Kafka Spout to ingest from */
		configureKafkaSpout(builder);
		
		/* configure DocumentStore bolt */
		configureDocumentStoreBolt(builder);
		
		/* configure IndexStore Bolt */
		configureIndexStoreBolt(builder);

		return builder.createTopology();
	}


	private void configureDocumentStoreBolt(TopologyBuilder builder) {
		DocumentStoreBolt docStoreBolt = new DocumentStoreBolt(topologyConfig);
		
		int docStoreBoltCount = Integer.valueOf(topologyConfig.getProperty("ecm.docstore.bolt.thread.count"));
		
		builder.setBolt("document_store_bolt", docStoreBolt, docStoreBoltCount).shuffleGrouping("kafkaSpout");
	}
	
	private void configureIndexStoreBolt(TopologyBuilder builder) {
	
		IndexStoreBolt indexStoreBolt = new IndexStoreBolt(topologyConfig);
		int indexStoreBoltCount = Integer.valueOf(topologyConfig.getProperty("ecm.indexstore.bolt.thread.count"));
		
		builder.setBolt("index_store_bolt", indexStoreBolt, indexStoreBoltCount).shuffleGrouping("kafkaSpout");
	}	

	private void buildAndSubmit() throws Exception {
		StormTopology topology = buildTopology();
		
		
		/* This conf is for Storm and it needs be configured with things like the following:
		 * 	Zookeeper server, nimbus server, ports, etc... All of this configuration will be picked up
		 * in the ~/.storm/storm.yaml file that will be located on each storm node.
		 */
		Config conf = new Config();
		conf.setDebug(true);	
		/* Set the number of workers that will be spun up for this topology. 
		 * Each worker represents a JVM where executor thread will be spawned from */
		Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("ecm.storm.document.topology.workers"));
		conf.put(Config.TOPOLOGY_WORKERS, topologyWorkers);
		
		String topologyName = topologyConfig.getProperty("ecm.topology.name");
		try {
			StormSubmitter.submitTopology(topologyName, conf, topology);	
		} catch (Exception e) {
			LOG.error("Error submiting Topology", e);
		}
			
	}

	private void configureKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = constructKafkaSpout();
		int spoutCount = Integer.valueOf(topologyConfig.getProperty("ecm.spout.thread.count"));		
		builder.setSpout("kafkaSpout", kafkaSpout, spoutCount);
	}


	/**
	 * Construct the KafkaSpout which comes from the jar storm-kafka-0.8-plus
	 * @return
	 */
	private KafkaSpout constructKafkaSpout() {
		KafkaSpout kafkaSpout = new KafkaSpout(constructKafkaSpoutConf());
		return kafkaSpout;
	}

	/**
	 * Construct 
	 * @return
	 */
	private SpoutConfig constructKafkaSpoutConf() {
		BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
		String topic = topologyConfig.getProperty("ecm.kafka.topic");
		String zkRoot = topologyConfig.getProperty("kafka.zookeeper.znode.parent");
		String consumerGroupId = topologyConfig.getProperty("ecm.kafka.consumer.group.id");
		String fetchSizeBytes = topologyConfig.getProperty("ecm.fetch.message.max.bytes");
		
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new DocumentScheme());
		// Set the following to consume large messages
		spoutConfig.fetchSizeBytes=Integer.valueOf(fetchSizeBytes);
		return spoutConfig;
	}
	
}



