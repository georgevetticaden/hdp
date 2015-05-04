package hortonworks.hdp.refapp.ecm.streaming.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import hortonworks.hdp.apputil.storm.StormTopologyParams;
import hortonworks.hdp.apputil.storm.StormUtils;
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
import org.junit.BeforeClass;
import org.junit.Test;

import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;

/**
 * This is Dev/Integration test where the topology is being deployed onto real "Dev" Cluster with running components like:
 * 	1. Zookeeper
 * 	2. Kafka
 * 	3. Storm
 * 	4. HBase
 * 	5. Solr
 * @author gvetticaden
 *
 */
public class DocumentProcessorTopologyDevClusterTest extends BaseTopologyTest {
	
	private static final Logger LOG = Logger.getLogger(DocumentProcessorTopologyDevClusterTest.class);
	private static final String STORM_TOPOLOGY_KEY = "ecm.storm.topology.jar";

	private static StormUtils stormUtils;

	@BeforeClass
	public static void setup() throws Exception {

		System.setProperty(BaseTopologyTest.DEPLOYMENT_ENV_KEY, "dev");
		// Set up system properties so right config gets picked up for registry
		setUpSystemRegistryConfigDirectoryLocation();
		
		//create AppContext
		createAppContext();
		
		//create StorUtils for deployment
		stormUtils = new StormUtils(getServiceRegistry());		
		
		//Populate the Topology config properties object from service registry
		populateTopologyConfig();
		
		//create Kafka Topics for test
		//Commetning out creating kafka topic. Assume it is created. Because delete topic is buggy in kafka .81
		//createKafkaTopic(topologyConfig.getProperty("ecm.kafka.topic"), 2, 2);
		
		// Initialize Kafka Producer to generate test data
		createKafkaProducer();		

		
	}
	
	
	/**
	 * Validates the lifecycle of the topology which includes:
	 * 	1. Deploying the Topology
	 * 	2. Verifying it is running
	 * 	3. Killing the Topology
	 * 	4. Verifying it is killed
	 * @throws Exception
	 */
	@Test
	public void testDeployAndKillTopology() throws Exception{
		
		
		String topologyName = topologyConfig.getProperty("ecm.topology.name");
		
		//kill Topology if it is running
		stormUtils.killStormTopology(topologyName);
		
		//Validate that the topology is not up.
		TopologySummary summary =  stormUtils.getStormTopologySummary(topologyName);
		assertTrue(summary == null || summary.get_status().equals("KILLED"));	
		
		//Deploy the topology
		StormTopologyParams topologyParams = createTopologyParams();
		stormUtils.deployStormTopology(topologyParams);
		
		//validate it is deployed
		summary =  stormUtils.getStormTopologySummary(topologyName);
		assertNotNull(summary);	
		assertNotNull(summary.get_id());
		assertThat(summary.get_status(), is("ACTIVE"));
		
		//Tear it down
		stormUtils.killStormTopology(topologyName);
		
		//validate that it was torn down..
		summary =  stormUtils.getStormTopologySummary(topologyName);
		assertTrue(summary == null || summary.get_status().equals("KILLED"));
		
	}
	
	@Test
	public void testDocumentTupleProcessing() throws Exception {
		
		//clean up the document store
		LOG.info("Truncating Document and Index Store");
		DocumentStoreUtils.truncate(getServiceRegistry());
		IndexStoreUtils.truncate(getServiceRegistry());
		
		
		//Validate that doc and index is empty
		DocumentStore docStore = context.getBean(DocumentStore.class);
		List<String> docKeys = docStore.getAllDocumentKeys();
		assertTrue(docKeys.isEmpty());
		
		IndexStore indexStore = context.getBean(IndexStore.class);
		assertTrue(indexStore.getAllDocumentKeys().isEmpty());
		
		String topologyName = topologyConfig.getProperty("ecm.topology.name");
		
		//kill Topology if it is running
		stormUtils.killStormTopology(topologyName);

		//Validate that the topology is not up.
		TopologySummary summary =  stormUtils.getStormTopologySummary(topologyName);
		assertTrue(summary == null || summary.get_status().equals("KILLED"));	
		
		//Deploy the topology
		StormTopologyParams topologyParams = createTopologyParams();
		stormUtils.deployStormTopology(topologyParams);
		
		//validate it is deployed
		summary =  stormUtils.getStormTopologySummary(topologyName);
		assertNotNull(summary);	
		assertNotNull(summary.get_id());
		assertThat(summary.get_status(), is("ACTIVE"));
		
		
		//send a Document Request to Kafka so it can be processed by the topology
		//Push NewDocumentRequest to Kafka
		String kafkaTopic = topologyConfig.getProperty("ecm.kafka.topic");
		LOG.info("Sending Document to Kafka");
		sendNewDocRequestToKafka(kafkaTopic);
		
		LOG.info("Waiting document is processed");
		Thread.sleep(50000);
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
	
	@Test
	public void testGetStormTopology() throws Exception {

		String topologyName = topologyConfig.getProperty("ecm.topology.name");
		LOG.info("Checking if topology["+topologyName + "] is running.");
		TopologySummary summary =  stormUtils.getStormTopologySummary(topologyConfig.getProperty("ecm.topology.name"));
		assertNotNull(summary);
		assertNotNull(summary.get_id());
	}	
	
	@Test
	public void testKillStormTopology() throws Exception {
		stormUtils.killStormTopology(topologyConfig.getProperty("ecm.topology.name"));
	}	

	private StormTopology createTopology(Properties topologyConfig) throws Exception {
		
		/* Construct the Topology */
		StormTopology topology = buildTopology(topologyConfig);
		return topology;
	}
			
	
	private StormTopology buildTopology(Properties topologyConfig) throws Exception {
		DocumentProcessorTopology docProcessorTopology = new DocumentProcessorTopology(topologyConfig);
		StormTopology topology = docProcessorTopology.buildTopology();
		return topology;
	}	
	
	private StormTopologyParams createTopologyParams() throws Exception {

		StormTopology topology = createTopology(topologyConfig);
		
		StormTopologyParams topologyParams = new StormTopologyParams();
		topologyParams.setUpload(true);	
		topologyParams.setTopology(topology);
		topologyParams.setTopologyName(topologyConfig.getProperty("ecm.topology.name"));
		topologyParams.setTopologyJarLocation(topologyConfig.getProperty(STORM_TOPOLOGY_KEY));
		return topologyParams;
	}	
	
	

}
