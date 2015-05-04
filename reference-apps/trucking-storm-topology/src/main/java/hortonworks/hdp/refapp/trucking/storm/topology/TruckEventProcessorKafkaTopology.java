package hortonworks.hdp.refapp.trucking.storm.topology;

import hortonworks.hdp.refapp.trucking.storm.bolt.alert.TruckEventRuleBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.hbase.TruckHBaseBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.hdfs.FileTimeRotationPolicy;
import hortonworks.hdp.refapp.trucking.storm.bolt.hive.HiveTablePartitionHiveServer2Action;
import hortonworks.hdp.refapp.trucking.storm.bolt.phoenix.TruckPhoenixHBaseBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.solr.SolrIndexingBolt;
import hortonworks.hdp.refapp.trucking.storm.bolt.websocket.WebSocketBolt;
import hortonworks.hdp.refapp.trucking.storm.kafka.TruckScheme2;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TruckEventProcessorKafkaTopology extends BaseTruckEventTopology {
	
	private static final Logger LOG = Logger.getLogger(TruckEventProcessorKafkaTopology.class);
	
	
	public TruckEventProcessorKafkaTopology(String configFileLocation) throws Exception {
		
		super(configFileLocation);			
	}
	
	public TruckEventProcessorKafkaTopology(Properties prop) throws Exception {
		
		this.topologyConfig = prop;		
	}	
	
	public StormTopology buildTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		
		/* Set up Kafka Spout to ingest from */
		configureKafkaSpout(builder);

		/* Set up HDFSBOlt to send every truck event to HDFS */
		configureHDFSBolt(builder);
		
		/* configure Solr indexing bolt */
		configureSolrIndexingBolt(builder);
		
		/* Setup Monitoring Bolt to track number of alerts per truck driver */
		configureMonitoringBolt(builder);
		
		/* Setup HBse Bolt for to persist violations and all events (if configured to do so)*/
		//configureHBaseBolt(builder);
		configureRealTimeBolt(builder);
		
		
		/* Setup WebSocket Bolt for alerts and notifications */
		configureWebSocketBolt(builder);	
		
		return builder.createTopology();
	}
	

	private void configureRealTimeBolt(TopologyBuilder builder) {
		boolean enablePhoenix = Boolean.valueOf(topologyConfig.getProperty("trucking.phoenix.enable")).booleanValue();
		
		if(enablePhoenix) {
			configurePhoenixHBaseBolt(builder);
			LOG.info("PHoenix enabled for real-time");
		} else {
			configureHBaseBolt(builder);
			LOG.info("HBase enabled for real-time");
		}
		
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
		Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("trucking.storm.trucker.topology.workers"));
		conf.put(Config.TOPOLOGY_WORKERS, topologyWorkers);
		
		try {
			StormSubmitter.submitTopology("truck-event-processor", conf, topology);	
		} catch (Exception e) {
			LOG.error("Error submiting Topology", e);
		}
			
	}

	private void configureSolrIndexingBolt(TopologyBuilder builder) {
		boolean isIndexingEnabled = Boolean.valueOf(topologyConfig.getProperty("trucking.solr.index.enable")).booleanValue();
		if(isIndexingEnabled) {
			LOG.info("Solr indexing enabled");
			int solrBoltCount = Integer.valueOf(topologyConfig.getProperty("trucking.solr.bolt.thread.count"));
			SolrIndexingBolt solrBolt = new SolrIndexingBolt(topologyConfig);
			builder.setBolt("solr_indexer_bolt", solrBolt, solrBoltCount).shuffleGrouping("kafkaSpout");
		} else {
			LOG.info("Solr indexing turned off");
		}
			
		
	}

	public void configureWebSocketBolt(TopologyBuilder builder) {
		boolean configureWebSocketBolt = Boolean.valueOf(topologyConfig.getProperty("trucking.notification.topic")).booleanValue();
		if(configureWebSocketBolt) {
			WebSocketBolt webSocketBolt = new WebSocketBolt(topologyConfig);
			builder.setBolt("web_sockets_bolt", webSocketBolt, 4).shuffleGrouping("hbase_bolt");
		}
	}

	public void configureHBaseBolt(TopologyBuilder builder) {
		TruckHBaseBolt hbaseBolt = new TruckHBaseBolt(topologyConfig);
		builder.setBolt("hbase_bolt", hbaseBolt, 2 ).shuffleGrouping("kafkaSpout");
	}
	
	public void configurePhoenixHBaseBolt(TopologyBuilder builder) {
		TruckPhoenixHBaseBolt hbaseBolt = new TruckPhoenixHBaseBolt(topologyConfig);
		builder.setBolt("phoenix_hbase_bolt", hbaseBolt, 2 ).shuffleGrouping("kafkaSpout");
	}	

	/**
	 * Send truckEvents from same driver to the same bolt instances to maintain accuracy of eventCount per truck/driver 
	 * @param builder
	 */
	public void configureMonitoringBolt(TopologyBuilder builder) {
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));
		builder.setBolt("monitoring_bolt", 
						new TruckEventRuleBolt(topologyConfig), boltCount)
						.fieldsGrouping("kafkaSpout", new Fields("driverId"));
	}

	public void configureHDFSBolt(TopologyBuilder builder) {
		// Use pipe as record boundary
		
		String rootPath = topologyConfig.getProperty("trucking.hdfs.path");
		String prefix = topologyConfig.getProperty("trucking.hdfs.file.prefix");
		String fsUrl = topologyConfig.getProperty("hdfs.url");
		String hiveServer2ConnectionString = topologyConfig.getProperty("hiveserver2.connect.string");
		String hiveServer2ConnectUser = topologyConfig.getProperty("trucking.hiveserver2.connect.user");
		String hiveStagingTableName = topologyConfig.getProperty("trucking.hive.staging.table.name");
		String databaseName = topologyConfig.getProperty("trucking.hive.database.name");
		Float rotationTimeInMinutes = Float.valueOf(topologyConfig.getProperty("trucking.hdfs.file.rotation.time.minutes"));
		
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");

		//Synchronize data buffer with the filesystem every 1000 tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// Rotate data files when they reach five MB
		//FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
		
		//Rotate every X minutes
		FileTimeRotationPolicy rotationPolicy = new FileTimeRotationPolicy(rotationTimeInMinutes, FileTimeRotationPolicy.Units.MINUTES);
		
		//Hive Partition Action
		
		HiveTablePartitionHiveServer2Action hivePartitionHiveServer2Action = new HiveTablePartitionHiveServer2Action(hiveServer2ConnectionString, hiveServer2ConnectUser, 
																												     hiveStagingTableName, databaseName, fsUrl);
		
		

		
		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
				.withPath(rootPath + "/staging")
				.withPrefix(prefix);

		// Instantiate the HdfsBolt
		HdfsBolt hdfsBolt = new HdfsBolt()
				 .withFsUrl(fsUrl)
		         .withFileNameFormat(fileNameFormat)
		         .withRecordFormat(format)
		         .withRotationPolicy(rotationPolicy)
		         .withSyncPolicy(syncPolicy)
		         .addRotationAction(hivePartitionHiveServer2Action);
				
		
		int hdfsBoltCount = Integer.valueOf(topologyConfig.getProperty("trucking.hdfsbolt.thread.count"));
		builder.setBolt("hdfs_bolt", hdfsBolt, hdfsBoltCount).shuffleGrouping("kafkaSpout");
	}

	public int configureKafkaSpout(TopologyBuilder builder) {
		KafkaSpout kafkaSpout = constructKafkaSpout();
		
		int spoutCount = Integer.valueOf(topologyConfig.getProperty("trucking.spout.thread.count"));
		int boltCount = Integer.valueOf(topologyConfig.getProperty("trucking.bolt.thread.count"));
		
		builder.setSpout("kafkaSpout", kafkaSpout, spoutCount);
		return boltCount;
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
		BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host"));
		String topic = topologyConfig.getProperty("trucking.kafka.topic");
		String zkRoot = topologyConfig.getProperty("kafka.zookeeper.znode.parent");
		String consumerGroupId = topologyConfig.getProperty("trucking.kafka.consumer.group.id");
		
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
		
		/* Custom TruckScheme that will take Kafka message of single truckEvent 
		 * and emit a 2-tuple consisting of truckId and truckEvent. This driverId
		 * is required to do a fieldsSorting so that all driver events are sent to the set of bolts */
		spoutConfig.scheme = new SchemeAsMultiScheme(new TruckScheme2());
		
		return spoutConfig;
	}
	
	public static void main(String[] args) throws Exception {
		String configFileLocation = args[0];
		TruckEventProcessorKafkaTopology truckTopology = new TruckEventProcessorKafkaTopology(configFileLocation);
		truckTopology.buildAndSubmit();
		
	}	

}



