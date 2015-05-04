package hortonworks.hdp.refapp.ecm.streaming;

import hortonworks.hdp.apputil.kafka.KafkaUtils;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.refapp.ecm.DeploymentEnv;
import hortonworks.hdp.refapp.ecm.config.TestECMStreamingHDPServiceRegistryConfig;
import hortonworks.hdp.refapp.ecm.registry.ECMBeanRefresher;
import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;
import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;
import hortonworks.hdp.refapp.ecm.service.config.DocumentStoreConfig;
import hortonworks.hdp.refapp.ecm.service.config.IndexStoreConfig;

import java.io.InputStream;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;



public class BaseTopologyTest {

	private static final Logger LOG = Logger.getLogger(BaseTopologyTest.class);
	
	public static final String DEPLOYMENT_ENV_KEY = "deployment.env";
	
	protected static AnnotationConfigApplicationContext context;	
	protected static Producer<String, DocumentUpdateRequest> kafkaProducer;
	
	//Properties and Service Registries
	protected static Properties topologyConfig;	
	
	/**
	 * Default to relative path and local config if the property has not been set as part of -D setting
	 */
	public static void setUpSystemRegistryConfigDirectoryLocation() {
		String serviceConfigDir = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		
		if(StringUtils.isEmpty(serviceConfigDir)) {
			DeploymentEnv deploymentEnv = DeploymentEnv.local;
			if(StringUtils.isNotEmpty(System.getProperty(DEPLOYMENT_ENV_KEY))) {
				deploymentEnv = DeploymentEnv.valueOf(System.getProperty(DEPLOYMENT_ENV_KEY));
			}
			System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/config/"+ deploymentEnv.toString() +"/registry");
			System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_IS_ABSOLUTE_PROP_KEY, "false");
		}

	}
	
	protected static HDPServiceRegistry getServiceRegistry() {
		return context.getBean(HDPServiceRegistry.class);
	}	
	
	protected static void createAppContext() {
		context = new AnnotationConfigApplicationContext();
		context.register(DocumentStoreConfig.class, IndexStoreConfig.class, TestECMStreamingHDPServiceRegistryConfig.class);
		context.refresh();
		
		ECMBeanRefresher refresher = new ECMBeanRefresher(context.getBean(HDPServiceRegistry.class), context);		
		refresher.refreshBeans();
		
	}	
	
	protected static void populateTopologyConfig() throws Exception {
		topologyConfig = constructStormTopologyConfig(getServiceRegistry());
	}	
	
	private static Properties constructStormTopologyConfig(HDPServiceRegistry registry)   {
		Properties topologyConfig = new Properties();
		
		// Dump everything from registry into the Topology Config
		for(String key: registry.getRegistry().keySet()) {
			topologyConfig.put(key, registry.getRegistry().get(key));
		}
		
		//Only extra we need to store is the kafka zookeeper connection string
		String zookeeperHostPort = registry.getKafkaZookeeperHost() + ":" + registry.getKafkaZookeeperClientPort();
		LOG.info("Kafka Zookeper host and port is: " + zookeeperHostPort);
		topologyConfig.put("kafka.zookeeper.host.port", zookeeperHostPort);
		
		return topologyConfig;
	}		
	
	protected void sendNewDocRequestToKafka(String kafkaTopic) throws Exception {
		DocumentUpdateRequest docRequest = constructDocumentUpdateRequest();
		KeyedMessage<String, DocumentUpdateRequest> message = new KeyedMessage<String, DocumentUpdateRequest>(kafkaTopic, docRequest);
		kafkaProducer.send(message);
	}	
	
	private DocumentUpdateRequest constructDocumentUpdateRequest() throws Exception {
		
		//generate request
		//byte[] document = getTestDocument("rfi/State Farm - NGIS RFP_Main_Hortonworks_Master Copy_02.27.15.docx");
		byte[] document = getTestDocument("rfi/Bell_RFI_Addendum_Hortonworks.pdf");

		DocumentMetaData docMetaData = createDocMetadataForStateFarmRFP();
		DocumentUpdateRequest docUpdateRequest = new DocumentUpdateRequest(null, document, docMetaData);

		return docUpdateRequest;
	}	

	private DocumentMetaData createDocMetadataForStateFarmRFP() {
		String customerName = "StateFarm";
		String documentName = "State Farm RFP";
		String mimeType = "application/pdf";
		String extension = ".pdf";
		DocumentClass docClassType = DocumentClass.RFP;
		DocumentMetaData docMeta = new DocumentMetaData(documentName, mimeType, docClassType, customerName, extension);
		return docMeta;
	}	
	
	protected byte[] getTestDocument(String fileName) throws Exception {
		InputStream document = this.getClass().getClassLoader().getResourceAsStream(fileName);
		return IOUtils.toByteArray(document);
	}	
	
	protected static void createKafkaTopic(String topicName, int replicationFactor, int numPartitions ) {
		KafkaUtils kafkaUtils = new KafkaUtils(getServiceRegistry());
		
		//first delete if it exists
		//kafkaUtils.deleteTopic(topicName);
		//create the the topics
		kafkaUtils.createTopic(topicName, replicationFactor, numPartitions);
		
		LOG.info("Kafa Topic[" + topicName + "] created");;
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}	
	
	protected static void createKafkaProducer() {
        Properties props = new Properties();
        String kafkaBrokerList = getServiceRegistry().getKafkaBrokerList();
        
        LOG.info("Kafka Broker List is: " + kafkaBrokerList);
        props.put("metadata.broker.list", getServiceRegistry().getKafkaBrokerList());

        props.put("serializer.class", "hortonworks.hdp.refapp.ecm.streaming.kafka.DocumentSerializer");
        props.put("request.required.acks", "1");
 
        ProducerConfig producerConfig = new ProducerConfig(props);		
        kafkaProducer = new Producer<String, DocumentUpdateRequest>(producerConfig);  
	}		
		
}
