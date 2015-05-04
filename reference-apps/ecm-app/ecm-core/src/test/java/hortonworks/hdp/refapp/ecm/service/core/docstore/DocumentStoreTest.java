package hortonworks.hdp.refapp.ecm.service.core.docstore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.BaseTest;
import hortonworks.hdp.refapp.ecm.registry.ECMBeanRefresher;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentProperties;
import hortonworks.hdp.refapp.ecm.util.DocumentGUIDUtils;
import hortonworks.hdp.refapp.ecm.util.DocumentStoreUtils;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;


public class DocumentStoreTest extends BaseTest{

	private static final Logger LOG = Logger.getLogger(DocumentStoreTest.class);
	
	@Autowired
	DocumentStore documentStore;
	
	@Autowired
	HDPServiceRegistry serviceRegistry;
	
	@Autowired
	ApplicationContext appContext;
	
	@Before
	public void setup() {
		ECMBeanRefresher beanRefresher = new ECMBeanRefresher(serviceRegistry, appContext );
		beanRefresher.refreshBeans();	
	}
	

	@Test
	public void testStoreDocument() throws Exception {

		//truncate Document Store First
		DocumentStoreUtils.truncate(serviceRegistry);		
		
		//store a new document
		DocumentDetails docDetailsRequest = addStateFarmRFPWithMetadataIntoIndexStore();

		//validate the documented was stored
		DocumentDetails docDetailsPersisted = documentStore.getDocument(docDetailsRequest.getDocKey());
		assertNotNull(docDetailsPersisted);
		assertNotNull(docDetailsPersisted.getDocContent());

	}
	
	@Test
	public void testGetDocument() throws Exception {
		
		//truncate Document Store First
		DocumentStoreUtils.truncate(serviceRegistry);		
		
		//store the doc
		DocumentDetails docDetailsRequest = addStateFarmRFPWithMetadataIntoIndexStore();
		
		//validate document retreived is correct
		//validate the documented was stored
		DocumentDetails docDetailsPersisted = documentStore.getDocument(docDetailsRequest.getDocKey());
		assertNotNull(docDetailsPersisted);
		assertNotNull(docDetailsPersisted.getDocContent());
		
		//Validate the Doc Properties got persisted
		DocumentProperties docProperties = docDetailsPersisted.getDocProperties();
		assertThat(docProperties.getMimeType(), is(docDetailsRequest.getDocProperties().getMimeType()));
		assertThat(docProperties.getName(), is(docDetailsRequest.getDocProperties().getName()));
		assertThat(docProperties.getExtension(), is(docDetailsRequest.getDocProperties().getExtension()));		
	}

	@Test
	public void testGetAllDocKeys() throws Exception {

		//truncate Document Store First
		DocumentStoreUtils.truncate(serviceRegistry);	
		
		//Should be empty to start off
		List<String> docKeys = documentStore.getAllDocumentKeys();
		assertTrue(docKeys.isEmpty());
		
		//Add a doc
		addStateFarmRFPWithMetadataIntoIndexStore();
		
		//Verify we get one key back
		docKeys = documentStore.getAllDocumentKeys();
		assertThat(docKeys.size(), is(1));
		assertNotNull(docKeys.get(0));
	}
	
	@Test
	public void dumpDocumentStore() throws Exception {

		//Should be empty to start off
		List<String> docKeys = documentStore.getAllDocumentKeys();
		LOG.info("Number of Docs in DocStore: " + docKeys.size());
		for(String docKey: docKeys) {
			DocumentDetails docDetailsPersisted  = documentStore.getDocument(docKey);
			LOG.info("Doc Key: "  + docDetailsPersisted.getDocKey());
			LOG.info("Doc Binary Content : " + docDetailsPersisted.getDocContent());
			DocumentProperties docProperties = docDetailsPersisted.getDocProperties();
			LOG.info("Doc Mime Type: " + docProperties.getMimeType());
			LOG.info("Doc Name is: " + docProperties.getName());
			LOG.info("Doc extension is: " + docProperties.getExtension());	
		}
	}	

	@Test
	public void testTruncate() {
		DocumentStoreUtils.truncate(serviceRegistry);
	}
	
	
	private DocumentDetails addStateFarmRFPWithMetadataIntoIndexStore() throws Exception{
		//Validate the Doc doesn't exist
		String key = DocumentGUIDUtils.generateDocGUID();
		DocumentDetails docDetails = documentStore.getDocument(key);
		assertNull(docDetails);
		
			
		//Create Doc Details Request to store		
		byte[] document = getTestDocument("rfi/State Farm - NGIS RFP_Main_Hortonworks_Master Copy_02.27.15.pdf");
		assertNotNull(document);
		String fileName = "State Farm RFP Document Response";
		String mimeType = "application/pdf";
		String extension = ".pdf";
		DocumentProperties docProperties = new DocumentProperties(fileName, mimeType, extension);
		docDetails = new DocumentDetails(key, document, docProperties);
			
		
		//Store the document
		documentStore.storeDocument(docDetails);
		
		return docDetails;
	}
	
	
	
	
}
