package hortonworks.hdp.refapp.ecm.service.api;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.List;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.BaseTest;
import hortonworks.hdp.refapp.ecm.registry.ECMBeanRefresher;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentProperties;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.SolrIndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIDocument;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIRepository;
import hortonworks.hdp.refapp.ecm.util.DocumentStoreUtils;
import hortonworks.hdp.refapp.ecm.util.IndexStoreUtils;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;


public class DocumentServiceTest extends BaseTest {

	private static final Logger LOG = Logger.getLogger(DocumentServiceTest.class);
	
	@Autowired
	DocumentServiceAPI documentService;
	
	@Autowired
	DocumentStore docStore;
	
	@Autowired
	IndexStore indexStore;
	
	@Autowired
	HDPServiceRegistry registry;
	
	@Autowired
	ApplicationContext appContext;

	
	@Before
	public void setup() {
		ECMBeanRefresher beanRefresher = new ECMBeanRefresher(registry, appContext);
		beanRefresher.refreshBeans();
	}

	
	@Test
	public void testTruncateStores() {
		truncateStores();
	}

	
	@Test
	public void dumpDocumentAndIndexStore() throws Exception {

		//Should be empty to start off
		List<String> docKeys = docStore.getAllDocumentKeys();
		LOG.info(" ---------- Document Store Details ----------------");
		LOG.info("Number of Docs in DocStore: " + docKeys.size());
		for(String docKey: docKeys) {
			DocumentDetails docDetailsPersisted  = docStore.getDocument(docKey);
			LOG.info("Doc Key: "  + docDetailsPersisted.getDocKey());
			LOG.info("Doc Binary Content : " + docDetailsPersisted.getDocContent());
			DocumentProperties docProperties = docDetailsPersisted.getDocProperties();
			LOG.info("Doc Mime Type: " + docProperties.getMimeType());
			LOG.info("Doc Name is: " + docProperties.getName());
			LOG.info("Doc extension is: " + docProperties.getExtension());	
		}
		
		LOG.info(" ---------- Index Store Details ----------------");
		docKeys = indexStore.getAllDocumentKeys();
		LOG.info("Number of Docs in Index Store: " + docKeys.size());
		for(String docKey: docKeys) {
			RFIDocument rfiDoc = indexStore.getDocument(docKey);
			
			LOG.info("Doc Key: " + rfiDoc.getId());
			LOG.info("Document Name: " + rfiDoc.getDocumentname());
			//LOG.info("Doc Binary Content: " + rfiDoc.getBody());		
			
			// Validate the Metadata was persisted
			LOG.info("Customer Name: " + rfiDoc.getCustomername());
			LOG.info("Mime Type: " + rfiDoc.getMimetype());
			LOG.info("Document Class: " + rfiDoc.getDocumentclass());
		}
		
					
		
		
	}		
	
	@Test
	public void testAddDocument() throws Exception {

		truncateStores();
		
		DocumentUpdateRequest docRequest = addStateFarmRFPDoc();
		
		//validate Doc is in ImageStore
		DocumentDetails docDetails = docStore.getDocument(docRequest.getDocKey());
		assertNotNull(docDetails.getDocContent());
		
		//validate Doc Index is in IndexStore
		RFIDocument indexDocument = indexStore.getDocument(docRequest.getDocKey());
		assertNotNull(indexDocument);
		assertNotNull(indexDocument.getBody());
		
	}

	
	@Test
	public void testGetDocument() throws Exception {

		truncateStores();
		
		DocumentUpdateRequest docRequest = addStateFarmRFPDoc();
		
		//validate Doc is in ImageStore
		DocumentDetails docDetailsPersisted = docStore.getDocument(docRequest.getDocKey());
		assertNotNull(docDetailsPersisted.getDocContent());
		
		//validate the details of the Document Details returned
		assertNotNull(docDetailsPersisted);
		assertNotNull(docDetailsPersisted.getDocContent());
		
		//Validate the Doc Properties got persisted
		DocumentProperties docProperties = docDetailsPersisted.getDocProperties();
		assertThat(docProperties.getMimeType(), is(docRequest.getDocMetadata().getMimeType()));
		assertThat(docProperties.getName(), is(docRequest.getDocMetadata().getDocumentName()));
		assertThat(docProperties.getExtension(), is(docRequest.getDocMetadata().getExtension()));				
		
		//validate Doc Index is in IndexStore
		RFIDocument rfiDoc = indexStore.getDocument(docRequest.getDocKey());
		assertNotNull(rfiDoc);
		assertNotNull(rfiDoc.getBody());
		
		//validate the details of the Index Details returned
		assertThat(rfiDoc.getCustomername(), is(docRequest.getDocMetadata().getCustomerName()));
		assertThat(rfiDoc.getMimetype(), is(docRequest.getDocMetadata().getMimeType()));
		assertThat(rfiDoc.getDocumentclass(), is(docRequest.getDocMetadata().getDocClassType().getValue()));
		assertThat(rfiDoc.getDocumentname(), is(docRequest.getDocMetadata().getDocumentName()));		
		
	}	

	
	private DocumentUpdateRequest addStateFarmRFPDoc() throws Exception {
		
		truncateStores();
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
	
	private RFIRepository getSolrIndexRepo() {
		return ((SolrIndexStore)indexStore).getSolrIndexRepo();
	}	
	
	private void truncateStores() {
		IndexStoreUtils.truncate(registry);
		DocumentStoreUtils.truncate(registry);
	}		

}
