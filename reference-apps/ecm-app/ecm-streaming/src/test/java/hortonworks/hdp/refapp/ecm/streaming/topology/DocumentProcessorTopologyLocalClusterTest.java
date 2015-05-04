package hortonworks.hdp.refapp.ecm.streaming.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentProperties;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIDocument;
import hortonworks.hdp.refapp.ecm.streaming.BaseTopologyTest;
import hortonworks.hdp.refapp.ecm.util.DocumentStoreUtils;
import hortonworks.hdp.refapp.ecm.util.IndexStoreUtils;

import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.sakserv.minicluster.impl.KafkaLocalBroker;
import com.github.sakserv.minicluster.impl.StormLocalCluster;
import com.github.sakserv.minicluster.impl.ZookeeperLocalCluster;


/**
 * A unit test with mini clusters starteted as part of the test which includes the followign: components:
 * 	1. Zookeeper
 * 	2. Kafka
 * 	3. Storm
 * 
 * Since MiniClusters don't currently exists for HBase and STorm right now, those enpoints have not been mocked out..
 * 
 * @author gvetticaden
 *
 */
public class DocumentProcessorTopologyLocalClusterTest extends BaseTopologyTest {
	
		
	private static final Logger LOG = Logger.getLogger(DocumentProcessorTopologyLocalClusterTest.class);

	// Local Clusters
	private static ZookeeperLocalCluster zookeeperLocalCluster;
	private static StormLocalCluster stormLocalCluster;	
	private static KafkaLocalBroker kafkaLocalBroker;

	@BeforeClass
	public static void setUp() throws Exception{
		
		// Set up system properties so right config gets picked up
		setUpSystemRegistryConfigDirectoryLocation();
		
		//Create the service registry to get all the configuration required for cluster and topology
		createAppContext();
		
		//construct the topology properties
		populateTopologyConfig();
		
		//start zookeeper
		startZookeeper();
		
		//start kafka
		startKafka();
		
		//create Kafka Topics for test
		createKafkaTopic(topologyConfig.getProperty("ecm.kafka.topic"), 1, 1);
		
		// Initialize Kafka Producer to generate test data
		createKafkaProducer();
		
		//start Storm
		startStorm();
	}


	/**
	 * Tests the document processor topology by passing a DocRequest through the topology 
	 * and validating that it was persisted into the Index and Document Store.
	 * @throws Exception
	 */
	@Test
	public void testDocumentProcessorTopology() throws Exception {

		//clean up the document store
		LOG.info("Truncating Document and Index Store");
		DocumentStoreUtils.truncate(getServiceRegistry());
		IndexStoreUtils.truncate(getServiceRegistry());
		
		
		//Validate that docStore is empty
		DocumentStore docStore = context.getBean(DocumentStore.class);
		List<String> docKeys = docStore.getAllDocumentKeys();
		assertTrue(docKeys.isEmpty());
		
		IndexStore indexStore = context.getBean(IndexStore.class);
		assertTrue(indexStore.getAllDocumentKeys().isEmpty());
		
		
		
		//Now Deploy the topology
		DocumentProcessorTopology docProcessorTopology = new DocumentProcessorTopology(topologyConfig);
		stormLocalCluster.submitTopology(
				topologyConfig.getProperty("ecm.topology.name"),
				stormLocalCluster.getConf(), docProcessorTopology.buildTopology());
		
		LOG.info("Waiting until Topology is fully initialized");
		Thread.sleep(10000);
		LOG.info("Done Waiting. Hopefully Topology is Initialized");
		
		//Push NewDocumentRequest to Kafka
		String kafkaTopic = topologyConfig.getProperty("ecm.kafka.topic");
		sendNewDocRequestToKafka(kafkaTopic);
		
		LOG.info("Waiting document is processed");
		Thread.sleep(10000);
		LOG.info("Done Waiting. Hopefully Document is processed");		
		
		//Validate that Doc was prpersisted into the Document Store
		docKeys = docStore.getAllDocumentKeys();
		assertThat(docKeys.size(), is(1));
		String docKey = docKeys.get(0);
		assertNotNull(docKey);
		
		//Get the Document Details and validates
		DocumentDetails docDetails = docStore.getDocument(docKey);
		assertNotNull(docDetails);
		assertNotNull(docDetails.getDocContent());
		
		//Validate the Doc Properties got persisted
		DocumentProperties docProperties = docDetails.getDocProperties();
		assertThat(docProperties.getMimeType(), is("application/pdf"));
		assertThat(docProperties.getName(), is("State Farm RFP"));
		assertThat(docProperties.getExtension(), is(".pdf"));		
		
		
		//Validate the Doc was added to the index Store
		docKeys = null;
		docKeys = indexStore.getAllDocumentKeys();
		assertThat(docKeys.size(), is(1));
		RFIDocument rfiDoc = indexStore.getDocument(docKeys.get(0));
		assertNotNull(rfiDoc);
		assertNotNull(rfiDoc.getId());
		assertNotNull(rfiDoc.getBody());		
		
		// Validate the Metadata was persisted
		assertThat(rfiDoc.getCustomername(), is("StateFarm"));
		assertThat(rfiDoc.getMimetype(), is("application/pdf"));
		assertThat(rfiDoc.getDocumentclass(), is(DocumentClass.RFP.getValue()));
		assertThat(rfiDoc.getDocumentname(), is("State Farm RFP"));		
		
	}

	@AfterClass
	public static void tearDown() {
	
		stormLocalCluster.stop(topologyConfig.getProperty("ecm.topology.name"));
		kafkaLocalBroker.stop();
		zookeeperLocalCluster.stop();
		
		LOG.info("Servers stopped");
	}


	
	
	private static void startStorm() {
		stormLocalCluster = new StormLocalCluster.Builder()
				.setZookeeperHost(getServiceRegistry().getKafkaZookeeperHost())
				.setZookeeperPort(Long.valueOf(getServiceRegistry().getKafkaZookeeperClientPort()))
				.setEnableDebug(Boolean.parseBoolean(topologyConfig.getProperty("storm.enable.debug")))
				.setNumWorkers(Integer.parseInt(topologyConfig.getProperty("ecm.storm.document.topology.workers")))
				.build();
		stormLocalCluster.start();
		
		LOG.info("Storm Cluster Started");
	}
	
	private static void startKafka() {

		String kafkaBrokerList = getServiceRegistry().getKafkaBrokerList();
		String[] kafkaHostAndPort = kafkaBrokerList.split(":");
		
		Properties configProperties = new Properties();
		//The following 2 props are required for large messages over 1 MB..Default is 1 MB so bumping it up to 10GB
		configProperties.put("message.max.bytes", "10000000");
		configProperties.put("replica.fetch.max.bytes", "10000000");
		kafkaLocalBroker = new KafkaLocalBroker.Builder()
				.setKafkaHostname(kafkaHostAndPort[0])
				.setKafkaPort(Integer.parseInt(kafkaHostAndPort[1]))
				.setKafkaBrokerId(
						Integer.parseInt(topologyConfig.getProperty("kafka.test.broker.id")))
				.setKafkaProperties(configProperties)
				.setKafkaTempDir(topologyConfig.getProperty("kafka.test.temp.dir"))
				.setZookeeperConnectionString(topologyConfig.getProperty("kafka.zookeeper.host.port"))
				.build();
		kafkaLocalBroker.start();
		
		LOG.info("Kafka Cluster Started");

	}	

	private static void startZookeeper() {
		zookeeperLocalCluster = new ZookeeperLocalCluster.Builder()
				.setPort(Integer.parseInt(getServiceRegistry().getKafkaZookeeperClientPort()))
				.setTempDir(topologyConfig.getProperty("zookeeper.temp.dir"))
				.setZookeeperConnectionString(topologyConfig.getProperty("kafka.zookeeper.host.port"))
				.build();
		zookeeperLocalCluster.start();
		
		LOG.info("Zookeeper Cluster Started");
	}	
		
	
}
