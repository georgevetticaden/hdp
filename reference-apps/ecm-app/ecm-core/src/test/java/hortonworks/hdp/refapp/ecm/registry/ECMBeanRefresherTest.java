package hortonworks.hdp.refapp.ecm.registry;

import static org.junit.Assert.assertNotNull;
import hortonworks.hdp.apputil.registry.DeploymentMode;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.apputil.registry.ServiceRegistryParams;
import hortonworks.hdp.refapp.ecm.config.ECMCustomHDPServiceRegistryConfig;
import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;
import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.api.DocumentServiceAPI;
import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;
import hortonworks.hdp.refapp.ecm.service.config.DocumentServiceConfig;
import hortonworks.hdp.refapp.ecm.service.config.DocumentStoreConfig;
import hortonworks.hdp.refapp.ecm.service.config.IndexStoreConfig;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIDocument;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class ECMBeanRefresherTest {

	private static final Logger LOG = Logger.getLogger(ECMBeanRefresherTest.class);

	private static final String SOLR_SERVER_URL = "http://vett-search01.cloud.hortonworks.com:8983/solr";
	private static final String ECM_SOLR_CORE = "rawdocs";
	public static final String SLIDER_HBASE_PUBLISHER_URL = "http://centralregion09.cloud.hortonworks.com:34112/ws/v1/slider/publisher";	
	
	private static DocumentServiceAPI documentService;
	private static DocumentStore docStore;
	private static IndexStore indexStore;
	private static HDPServiceRegistry registry;
	
	
	@Autowired
	private static AnnotationConfigApplicationContext context;
	

	@BeforeClass
	public static void setUpSystemProperties() {
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/config/dev/registry/empty");
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_IS_ABSOLUTE_PROP_KEY, "false");	

		/* Create the App Context. Which will create an emtpy Service Registry to start with that we will fresh */
		createAppContext();		
	}

	private static void createAppContext() {
		context = new AnnotationConfigApplicationContext();
		context.register(DocumentStoreConfig.class, IndexStoreConfig.class, DocumentServiceConfig.class, ECMCustomHDPServiceRegistryConfig.class);
		context.refresh();
		
		documentService = context.getBean(DocumentServiceAPI.class);
		docStore = context.getBean(DocumentStore.class);
		indexStore = context.getBean(IndexStore.class);
		registry = context.getBean(HDPServiceRegistry.class);
	}
	
	@Test
	public void testRefreshBeans() throws Exception {
			
		//Add A document via the DocumentServiceAPI (that calls IndexStore and DocuemntStore) without updating context. THis shoudl throw an error
		try {
			//addStateFarmRFPDoc();
			//Assert.fail("Exception should have been thrown since registry is empty for INdexStore and DocumentStore to work with");
		} catch (Exception e) {
			//HBase hangs as opposed to throw an exception when it can't connect so why its commented out..
			//expected exception
		}
		
		//Now refresh the registry with the necessary configuration (HBase and Solr configuration)
		refreshRegistry();
		
		//Now Update necessary beans in appContext based on refresh registry
		ECMBeanRefresher ecmBeanRefresher = new ECMBeanRefresher(registry, context);
		ecmBeanRefresher.refreshBeans();

		// Now that the bean is refresh. The same call to add doc shoudl suceed
		DocumentUpdateRequest docRequest = addStateFarmRFPDoc();
		
		//validate Doc is in ImageStore
		DocumentDetails docDetails = docStore.getDocument(docRequest.getDocKey());
		assertNotNull(docDetails.getDocContent());
		
		//validate Doc Index is in IndexStore
		RFIDocument indexDocument = indexStore.getDocument(docRequest.getDocKey());
		assertNotNull(indexDocument);
		assertNotNull(indexDocument.getBody());		
		
	}



	private void refreshRegistry() throws Exception {
	
		Map<String, String> customParams = new HashMap<String, String>();
		customParams.put(ECMRegistryKeys.SOLR_SERVER_URL, SOLR_SERVER_URL);
		customParams.put(ECMRegistryKeys.ECM_SOLR_CORE, ECM_SOLR_CORE);
		registry.populate(createServiceRegistryParams(), customParams, null);
	}
	

	
	
	private DocumentUpdateRequest addStateFarmRFPDoc() throws Exception {
		
		//generate request
		byte[] document = getTestDocument("rfi/HortonworksRFIResponsetoUSCellular.pdf");
		

		DocumentMetaData docMetaData = createDocMetadataForStateFarmRFP();
		DocumentUpdateRequest docUpdateRequest = new DocumentUpdateRequest(null, document, docMetaData);

		//add teh document
		String key = documentService.addDocument(docUpdateRequest);
		LOG.info("Document created with key["+key + "]");
		
		docUpdateRequest.setDocKey(key);
		
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
	
	private ServiceRegistryParams createServiceRegistryParams() {
		ServiceRegistryParams params = new ServiceRegistryParams();
		params.setHbaseDeploymentMode(DeploymentMode.SLIDER);
		params.setHbaseSliderPublisherUrl(SLIDER_HBASE_PUBLISHER_URL);
		
		
		return params;
	}	

	protected byte[] getTestDocument(String fileName) throws Exception {
		InputStream document = this.getClass().getClassLoader().getResourceAsStream(fileName);
		return IOUtils.toByteArray(document);
	}		

}
