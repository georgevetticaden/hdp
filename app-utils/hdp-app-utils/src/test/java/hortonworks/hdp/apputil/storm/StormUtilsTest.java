//package hortonworks.hdp.apputil.storm;
//
//import static org.junit.Assert.assertNotNull;
//import hortonworks.hdp.apputil.BaseUtilsTest;
//import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
//import hortonworks.hdp.refapp.trucking.storm.topology.TruckEventProcessorKafkaTopology;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.util.Properties;
//
//import org.apache.log4j.Logger;
//import org.junit.Before;
//import org.junit.Test;
//
//import backtype.storm.generated.StormTopology;
//import backtype.storm.generated.TopologySummary;
//
//
//public class StormUtilsTest extends BaseUtilsTest {
//	
//	private static final String STORM_CONFIG_LOCATION = "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/sedev/coe/hdp-app-utils/src/test/resources/storm/storm-topology-config.properties";
//	private static final String ACTIVE_MQ_CONNECTION_URL = "tcp://george-activemq01.cloud.hortonworks.com:61616";
//	private static final String TOPOLOGY_NAME = "truck-event-processor";
//	private static final Logger LOG = Logger.getLogger(StormUtilsTest.class);
//	private static final String SOLR_SERVER_URL = "http://george-search01.cloud.hortonworks.com:8983/solr";
//	private static final String STORM_TOPOLOGY_KEY = "storm.topology.jar";
//
//	
//	private StormUtils stormUtils;
//	HDPServiceRegistry serviceRegistry;
//	
//	@Before
//	public void initialize() throws Exception {
//
//		this.serviceRegistry = createHDPServiceRegistry();
//		this.stormUtils = new StormUtils(serviceRegistry);
//	}
//	
//	
//	@Test
//	public void testDeployServiceUpload() throws Exception{
//		
//		String stormConfigFileLocation = STORM_CONFIG_LOCATION;
//		Properties topologyConfig = constructStormTopologyConfig(stormConfigFileLocation, this.serviceRegistry);
//				
//		StormTopology topology = createTopology(topologyConfig);
//		
//		StormTopologyParams topologyParams = new StormTopologyParams();
//		topologyParams.setUpload(true);	
//		topologyParams.setTopology(topology);
//		topologyParams.setTopologyName(TOPOLOGY_NAME);
//		topologyParams.setTopologyJarLocation(topologyConfig.getProperty(STORM_TOPOLOGY_KEY));
//		
//		stormUtils.deployStormTopology(topologyParams);
//	}
//
//
//
////	@Test
////	public void testDeployServiceNoUpload() throws Exception{
////		
////		StormTopology topology = createTopology();
////		
////		StormTopologyParams topologyParams = new StormTopologyParams();
////		topologyParams.setUpload(false);	
////		topologyParams.setUploadedTopologyJarLocation("/mnt/hadoop/storm/nimbus/staging/stormjar-ac1fe301-3883-41a2-9962-8e258539e50b.jar");
////		topologyParams.setTopology(topology);
////		topologyParams.setTopologyName(TOPOLOGY_NAME);
////				
////		stormUtils.deployStormTopology(topologyParams);
////	}
//	
//	@Test
//	public void testGetStormTopology() throws Exception {
//
//		TopologySummary summary =  stormUtils.getStormTopologySummary(TOPOLOGY_NAME);
//		assertNotNull(summary);
//		System.out.println(summary.get_id());
//	}	
//	
//	@Test
//	public void testKillStormTopology() throws Exception {
//		stormUtils.killStormTopology(TOPOLOGY_NAME);
//	}
//
//	
//	private StormTopology createTopology(Properties topologyConfig) throws Exception {
//
//		/* Construct the Topology */
//		StormTopology topology = buildTopology(topologyConfig);
//		return topology;
//	}
//		
//	private Properties constructStormTopologyConfig(String configFileLocation, HDPServiceRegistry registry)   {
//		Properties topologyConfig = new Properties();
//		try {
//			/* populate topology config from properties file first */
//			topologyConfig.load(new FileInputStream(configFileLocation));
//			
//			/* Then populate configuration from the Service Registry for things like server names and connection strings */
//			populateStormTopologyConfigFromServiceRegistry(topologyConfig, registry);
//			
//			LOG.info(topologyConfig);
//		} catch (FileNotFoundException e) {
//			String errorMsg = "Encountered error while reading configuration properties: "
//					+ e.getMessage();
//			LOG.error(errorMsg);
//			throw new RuntimeException(errorMsg, e);
//		} catch (IOException e) {
//			String errMsg = "Encountered error while reading configuration properties: "
//					+ e.getMessage();
//			LOG.error(errMsg);
//			throw new RuntimeException(errMsg, e);
//		}	
//		return topologyConfig;
//	}	
//	
//	private StormTopology buildTopology(Properties topologyConfig) throws Exception {
//		TruckEventProcessorKafkaTopology truckTopology = new TruckEventProcessorKafkaTopology(topologyConfig);
//		StormTopology topology = truckTopology.buildTopology();
//		return topology;
//	}	
//	
//	private void populateStormTopologyConfigFromServiceRegistry(
//			Properties topologyConfig, HDPServiceRegistry serviceRegistry) {
//		topologyConfig.put("notification.topic.connection.url", ACTIVE_MQ_CONNECTION_URL);
//		String zookeeperHostPort = serviceRegistry.getKafkaZookeeperHost() + ":" + serviceRegistry.getKafkaZookeeperClientPort();
//		topologyConfig.put("kafka.zookeeper.host.port", zookeeperHostPort);
//		topologyConfig.put("kafka.zkRoot", serviceRegistry.getKafkaZookeeperZNodeParent());
//		topologyConfig.put("hdfs.url", serviceRegistry.getHDFSUrl());
//		topologyConfig.put("hbase.phoenix.url", serviceRegistry.getPhoenixConnectionURL());
//		topologyConfig.put("hbase.zookeeper.host", serviceRegistry.getHBaseZookeeperHost());
//		topologyConfig.put("hbase.zookeeper.client.port", serviceRegistry.getHBaseZookeeperClientPort());
//		topologyConfig.put("hbase.zookeeper.znode.parent", serviceRegistry.getHBaseZookeeperZNodeParent());
//		topologyConfig.put("hive.metastore.url", serviceRegistry.getHiveMetaStoreUrl());
//		topologyConfig.put("hiveserver2.connection.url", serviceRegistry.getHiveServer2ConnectionURL());
//		topologyConfig.put("solr.server.url", SOLR_SERVER_URL);
//		//topologyConfig.put("mail.smtp.host", MAIL_SMTP_HOST);
//		
//	}	
//
//}
