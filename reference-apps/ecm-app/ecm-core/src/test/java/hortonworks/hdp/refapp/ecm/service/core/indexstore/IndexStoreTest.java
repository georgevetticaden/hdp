package hortonworks.hdp.refapp.ecm.service.core.indexstore;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.BaseTest;
import hortonworks.hdp.refapp.ecm.registry.ECMBeanRefresher;
import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentIndexDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentSearchRequest;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.IndexMetaData;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.QueryResult;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIDocument;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIRepository;
import hortonworks.hdp.refapp.ecm.util.DocumentGUIDUtils;
import hortonworks.hdp.refapp.ecm.util.IndexStoreUtils;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

public class IndexStoreTest extends BaseTest {

	private static final Logger LOG = Logger.getLogger(IndexStoreTest.class);
	
	@Autowired
	IndexStore indexStore;
	
	@Autowired
	HDPServiceRegistry serviceRegistry;
	
	@Autowired
	ApplicationContext appContext;
	
	
	@Before
	public void cleanUp() {
		ECMBeanRefresher beanRefresher = new ECMBeanRefresher(serviceRegistry, appContext );
		beanRefresher.refreshBeans();	
		
		//truncate Index Store First
		IndexStoreUtils.truncate(serviceRegistry);
	}

	@Test
	public void testAddDocumentWithNoMetadata()  throws Exception {
		
		
		String docId = addUsCellularRFPWithNoMetadataIntoIndexStore();
		
		/* Fetch the document */
		RFIDocument rfiDoc = indexStore.getDocument(docId);
		assertNotNull(rfiDoc);
		assertThat(rfiDoc.getId(), is(docId));
		assertNotNull(rfiDoc.getBody());
		
		
		/* Delete the  document */
		indexStore.removeDocument(docId);
		
		/* Validate it was deleted */
		rfiDoc = indexStore.getDocument(docId);
		assertNull(rfiDoc);	

	}


	
	@Test
	public void testAddDocumentWithMetadata()  throws Exception {
		
		IndexMetaData indexMetaData = createIndexMetaData();
		
		//Add Document to Index Store
		String docId = addStateFarmRFPWithMetadataIntoIndexStore(indexMetaData);
		
		// Validate the that document was persisted
		RFIDocument rfiDoc = indexStore.getDocument(docId);
		assertNotNull(rfiDoc);
		assertThat(rfiDoc.getId(), is(docId));
		assertNotNull(rfiDoc.getBody());		
		
		// Validate the Metadata was persisted
		assertThat(rfiDoc.getCustomername(), is(indexMetaData.getCustomerName()));
		assertThat(rfiDoc.getMimetype(), is(indexMetaData.getMimeType()));
		assertThat(rfiDoc.getDocumentclass(), is(indexMetaData.getDocClassType().getValue()));
		assertThat(rfiDoc.getDocumentname(), is(indexMetaData.getDocumentName()));

		
	}
	
	@Test
	public void testAddingDocX() throws Exception{
	
		IndexMetaData docMetaData = createIndexMetaDataForStateFarmDocX();
		
		//Add Document to Index Store
		String docId = addStateFarmRFPDocXWithMetadataIntoIndexStore(docMetaData);
		
		// Validate that .docX has additional metadata populated like author and date from Tika
		RFIDocument rfiDoc = indexStore.getDocument(docId);
		assertNotNull(rfiDoc);
		assertThat(rfiDoc.getId(), is(docId));
		assertNotNull(rfiDoc.getBody());
		assertThat(rfiDoc.getLast_author(), is("George Vetticaden"));
		assertThat(rfiDoc.getPublisher(), is("Hortonworks"));
		assertNotNull(rfiDoc.getDate());
		LOG.info("RFP Doc Date is: " + rfiDoc.getDate());
		
		
	}	


	@Test
	public void testSearchAllWithNoResult() throws Exception {
	
		IndexMetaData docMetaData = createIndexMetaData();
		
		//Add Document to Index Store
		String docId = addStateFarmRFPWithMetadataIntoIndexStore(docMetaData);
		
		//Search the Document with something that doesn't exist
		DocumentSearchRequest searchRequest = createSearchRequest("cellular", null, null);
		QueryResult result = indexStore.searchAll(searchRequest);
		assertThat(result.getNumFound(), is(0L));		
		
	}
	

	
	@Test
	public void testSearchAllWithResult() throws Exception {
	
		IndexMetaData docMetaData = createIndexMetaData();;
		
		//Add Document to Index Store
		String docId = addStateFarmRFPWithMetadataIntoIndexStore(docMetaData);
		
		//Search the Document with something that doesn't exist
		String searchString = "Ranger";
		DocumentSearchRequest searchRequest = createSearchRequest(searchString, null, null);
		QueryResult result = indexStore.searchAll(searchRequest);
		assertThat(result.getNumFound(), is(1L));		
		
		// Validate the single result
		List<Map<String, String>> results = result.getResults();
		assertThat(results.size(), is(1));
		
		Map<String, String> firstResult = results.get(0);
		String body = firstResult.get("body");
		assertTrue(body.contains(searchString));
		assertThat(firstResult.get("customername"), is(docMetaData.getCustomerName()));
		assertThat(firstResult.get("mimetype"), is(docMetaData.getMimeType()));
		assertThat(firstResult.get("documentclass"), is(docMetaData.getDocClassType().getValue()));
		assertThat(firstResult.get("documentname"), is(docMetaData.getDocumentName()));			
		//assertThat(firstResult.get("last_author"), is(docMetaData.getDocumentName()));
		
		for(String key: firstResult.keySet()) {
			if(!key.equals("body"))
				//LOG.info("Key: " + key + ", Value: " + firstResult.get(key));
				LOG.info("Key: " + key);
		}
		
		//validate the Facets
		Map<String, Map<String, Long>> facetResults = result.getFacets();
		assertNotNull(facetResults);
		assertThat(facetResults.keySet().size(), is(1));
		for(String facetKey: facetResults.keySet()) {
			LOG.info("FacetKey:" + facetKey);
			Map<String, Long> facetValues = facetResults.get(facetKey);
			assertNotNull(facetValues);
			
			for(String facetValueKey: facetValues.keySet()) {
				LOG.info("FacetValueKey: " + facetValueKey);
				LOG.info("FacetValue Count: " + facetValues.get(facetValueKey));
			}
		}		
		
		//Validate the highlights
		
//		Map<String, List<List<String>>> highlights = result.getHighlights();
//		assertNotNull(highlights);
//		assertThat(highlights.keySet().size(), is(2));
//		for(String docHighlightKey: highlights.keySet()) {
//			LOG.info("Doc Highlight Key: " + docHighlightKey);
//			List<List<String>> docHighlightValue = highlights.get(docHighlightKey);
//			assertThat(docHighlightValue.size(), is(1));
//			List<String> firstDocHighLightValue = docHighlightValue.get(0);
//			for(String docHighligtValue: firstDocHighLightValue) {
//				LOG.info(docHighlightValue);
//			}
//		}		
		
		
	}	
	
	@Test
	public void testSearchByCustomerName()  throws Exception {
		
		IndexMetaData docMetaData = createIndexMetaData();
		
		//Add Document to Index Store
		String docId = addStateFarmRFPWithMetadataIntoIndexStore(docMetaData);
		
		
		// search by Customer Name
		List<RFIDocument> docs = indexStore.searchByCustomerName(docMetaData.getCustomerName());
		assertThat(docs.size(), is(1));
		RFIDocument rfiDoc = docs.get(0);
		assertThat(rfiDoc.getCustomername(), is(docMetaData.getCustomerName()));
		assertThat(rfiDoc.getMimetype(), is(docMetaData.getMimeType()));
		assertThat(rfiDoc.getDocumentclass(), is(docMetaData.getDocClassType().getValue()));
		assertThat(rfiDoc.getDocumentname(), is(docMetaData.getDocumentName()));
		
	}	
	
	@Test
	public void testGetAllDocumentKeys() throws Exception {
		IndexMetaData docMetaData = createIndexMetaData();
		
		//Add Document to Index Store
		String docId = addStateFarmRFPWithMetadataIntoIndexStore(docMetaData);
				
		List<String> docKeys = indexStore.getAllDocumentKeys();
		assertNotNull(docKeys);
		assertThat(docKeys.size(), is(1));
		assertThat(docKeys.get(0), is(docId));
	}
	
	
	@Test
	public void truncateIndexStore() throws Exception{
		Long numOfDocs = getSolrIndexRepo().count();
		LOG.info("About to delate all " + numOfDocs + " docs in the Index STore");
		
		getSolrIndexRepo().deleteAll();
		
		assertThat(getSolrIndexRepo().count(), is(0L));
		
	}	

	


	private DocumentSearchRequest createSearchRequest(String searchValue, String filter,
			Integer start) {
		DocumentSearchRequest searchRequest = new DocumentSearchRequest();
		searchRequest.setSearchValue(searchValue);
		searchRequest.setFilter(filter);
		searchRequest.setStart(start);
		searchRequest.setFacetField("last_author");
		searchRequest.setHighlightField("body");
		return searchRequest;
	}		
	

	
	private String addUsCellularRFPWithNoMetadataIntoIndexStore() throws Exception {
		byte[] document = getTestDocument("rfi/HortonworksRFIResponsetoUSCellular.pdf");
		assertNotNull(document);
		String docId  = DocumentGUIDUtils.generateDocGUID();
		DocumentIndexDetails docUpdateRequest = createDocIndexDetails(docId, document, null);
		
		/* INdex a document */
		indexStore.addDocument(docUpdateRequest);
		return docId;
	}	
	
	private String addStateFarmRFPWithMetadataIntoIndexStore(IndexMetaData docMeta) throws Exception {
		byte[] document = getTestDocument("rfi/State Farm - NGIS RFP_Main_Hortonworks_Master Copy_02.27.15.pdf");
		assertNotNull(document);
		String docId  = "HortonworksRFIResponsetoUSCellular.pdf";
		

		DocumentIndexDetails docUpdateRequest = createDocIndexDetails(docId, document, docMeta);
		
		/* INdex a document */
		indexStore.addDocument(docUpdateRequest);
		
		return docId;
		

	}	
	
	private String addStateFarmRFPDocXWithMetadataIntoIndexStore(IndexMetaData docMeta) throws Exception {
		byte[] document = getTestDocument("rfi/HortonworksRFIResponsetoUSCellular.docx");
		assertNotNull(document);
		String docId  = DocumentGUIDUtils.generateDocGUID();
		

		DocumentIndexDetails docUpdateRequest = createDocIndexDetails(docId, document, docMeta);
		
		/* INdex a document */
		indexStore.addDocument(docUpdateRequest);
		
		return docId;
		

	}	

	

	private IndexMetaData createIndexMetaDataForStateFarmDocX() {
		String customerName = "StateFarm";
		String documentName = "State Farm RFP";
		String mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
		DocumentClass docClassType = DocumentClass.RFP;
		IndexMetaData indexMetaData = new IndexMetaData(documentName, mimeType, docClassType, customerName);
		return indexMetaData;
	}
	
	private IndexMetaData createIndexMetaData() {
		String customerName = "StateFarm";
		String documentName = "State Farm RFP";
		String mimeType = "application/pdf";
		DocumentClass docClassType = DocumentClass.RFP;
		IndexMetaData indexMetaData = new IndexMetaData(documentName, mimeType, docClassType, customerName);
		return indexMetaData;
	}	

	private RFIRepository getSolrIndexRepo() {
		return ((SolrIndexStore)indexStore).getSolrIndexRepo();
	}		

}
