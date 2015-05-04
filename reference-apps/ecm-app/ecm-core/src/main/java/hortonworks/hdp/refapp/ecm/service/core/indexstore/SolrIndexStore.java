package hortonworks.hdp.refapp.ecm.service.core.indexstore;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.registry.ECMRegistryKeys;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentIndexDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentSearchRequest;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.IndexMetaData;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.QueryResult;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIDocument;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIRepository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.solr.repository.support.SolrRepositoryFactory;


public class SolrIndexStore implements IndexStore {

	private static final Logger LOG = Logger.getLogger(SolrIndexStore.class);
	
	private SolrServer solrServer;
	private RFIRepository solrIndexRepo;

	private HDPServiceRegistry serviceRegistry;

	public SolrIndexStore(HDPServiceRegistry serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	public void initialize() {
		String url = serviceRegistry.getCustomValue(ECMRegistryKeys.SOLR_SERVER_URL) + "/" + serviceRegistry.getCustomValue(ECMRegistryKeys.ECM_SOLR_CORE);
	    this.solrServer = new HttpSolrServer(url);
	    RepositoryFactorySupport solrSupport = new SolrRepositoryFactory(this.solrServer);
	    this.solrIndexRepo = solrSupport.getRepository(RFIRepository.class);
	    
	    
	}
	


	/**
	 * Adds the Document to the index Store for indexing
	 */
	@Override
	public void addDocument(DocumentIndexDetails doc) throws Exception {
		ContentStreamUpdateRequest req = constructContentStreamUpdateRequest(doc);
		if(doc.getIndexMetaData() != null)
			populateMetaDataIntoRequest(req, doc.getIndexMetaData());
		solrServer.request(req);
	}


	
	/**
	 * Gets a Document from the Index Store base don id
	 */
	@Override
	public RFIDocument getDocument(String id) throws Exception {
		return solrIndexRepo.findById(id);
	}
	
		
	/**
	 * Removes a document form teh Index Store
	 */
	@Override
	public void removeDocument(String id) throws Exception {
		solrIndexRepo.delete(id);
		
	}
	
	/**
	 * Searches for Documents in the INdex Store by searching for the value in the document content
	 */
	@Override
	public List<RFIDocument> searchByBody(String searchValue) throws Exception {
		List<RFIDocument> docs = solrIndexRepo.findByBody(searchValue);
		return docs;
	}	
	
	/**
	 * Searches for Docuemnts by the customer name Metadata
	 */
	@Override
	public List<RFIDocument> searchByCustomerName(String customer) throws Exception {
		List<RFIDocument> docs = solrIndexRepo.findByCustomername(customer);
		return docs;
	}	
	
	/**
	 * Searches for Documents across the document content and all metadata
	 */
	@Override
	public QueryResult searchAll(DocumentSearchRequest request)
			throws Exception {
		SolrQuery solrQuery = new SolrQuery();
	    solrQuery.setQuery(request.getSearchValue());
	    
	    if (request.getStart() != null && request.getStart() > 0) 
	    	solrQuery.setStart(request.getStart());
	    
	    String facetField = request.getFacetField() ;
	    if (StringUtils.isNotEmpty(facetField)) {
	      solrQuery.setFacet(true);
	      solrQuery.addFacetField(facetField);
	      solrQuery.addHighlightField(request.getHighlightField());
	      solrQuery.setHighlightFragsize(200);
	      solrQuery.setHighlightSnippets(100);
	    }		
		
	    QueryResponse response = solrServer.query(solrQuery);
	    SolrDocumentList responseList = response.getResults();
	    
	    List<Map<String, String>> queryResponseResults = new ArrayList<Map<String,String>>();
	    for(SolrDocument doc: responseList) {
	    	Map<String, String> docFieldValuesMap = new HashMap<String, String>();
	    	for(String fieldName: doc.getFieldNames()) {
	    		LOG.info("Field in searchAll: " + fieldName);
	    		String fieldValueString = "";
	    		Object fieldValueObject = doc.getFieldValue(fieldName);
	    		if( fieldValueObject != null) {
	    			fieldValueString = fieldValueObject.toString();
	    		}
	    		docFieldValuesMap.put(fieldName, fieldValueString);
	    	}
	    	queryResponseResults.add(docFieldValuesMap);
	    }
	    
	      
	    Map<String, Map<String, Long>> facetsResult = new HashMap<String , Map<String, Long>>(); 
	    if(response.getFacetFields() != null) {
		    for(FacetField facet: response.getFacetFields()) {
		    	Map<String, Long> facetValueMapResult = new HashMap<String, Long>();
		    	
		    	List<Count> facetValues = facet.getValues();
		    	for(Count facetValue: facetValues) {
		    		
		    		facetValueMapResult.put(facetValue.getName(), facetValue.getCount());
		    	}
		    	facetsResult.put(facet.getName(), facetValueMapResult);
		    }	    	
	    }

	    Map<String, List<List<String>>> highlightResults = new HashMap<String, List<List<String>>>();
	    if(response.getHighlighting() != null) {
	    	Map<String, Map<String, List<String>>> highlights = response.getHighlighting();
	    	
	    	for(String docHighlightKey: highlights.keySet()) {
	    		List<List<String>> docHighlightListResult = new ArrayList<List<String>>();
	    		
	    		Map<String, List<String>> docHighlightValue = highlights.get(docHighlightKey);
	    		for(String docHighlightField: docHighlightValue.keySet()) {
	    			docHighlightListResult.add(docHighlightValue.get(docHighlightField));
	    		}
	    		
	    		highlightResults.put(docHighlightKey, docHighlightListResult);
	    	}
	    }
	    
	    return new QueryResult(responseList.getNumFound(), queryResponseResults, responseList.getStart(), facetsResult, highlightResults);
	}


	/**
	 * Returns all the document keys in the Index Store
	 */
	public List<String> getAllDocumentKeys() throws Exception {
		//TODO: Need to find a more efficeint way to do this.
		Iterable<RFIDocument> docs = solrIndexRepo.findAll();
		List<String> docKeys = new ArrayList<String>();
		for(RFIDocument doc: docs) {
			docKeys.add(doc.getId());
		}
		return docKeys;
	}		
	
	
	private ContentStream createContentStream(final String id, final byte[] document, final String mimeType) {
		
		ContentStream contentStream = new ContentStream() {
			
			@Override
			public InputStream getStream() throws IOException {
				return new ByteArrayInputStream(document);
			}
			
			@Override
			public String getSourceInfo() {
				// TODO Auto-generated method stub
				return id;
			}
			
			@Override
			public Long getSize() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Reader getReader() throws IOException {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public String getName() {
				return id;
			}
			
			@Override
			public String getContentType() {
				return mimeType;
			}
		};
		return contentStream;
	}


	private void populateMetaDataIntoRequest(ContentStreamUpdateRequest req,
			IndexMetaData docMeta) {
		req.setParam("literal.customername", docMeta.getCustomerName());
		req.setParam("literal.documentname", docMeta.getDocumentName());
		req.setParam("literal.documentclass", docMeta.getDocClassType().getValue());
		req.setParam("literal.mimetype", docMeta.getMimeType());
	}

	private ContentStreamUpdateRequest constructContentStreamUpdateRequest(DocumentIndexDetails doc) {
		ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/extract");
		req.setParam("literal.id", doc.getDocKey());
		req.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
		req.setParam("fmap.content", "body_s");
		req.setParam("fmap.author", "last-author_s");
		String mimeType = null;
		if(doc.getIndexMetaData() != null) {
			mimeType = doc.getIndexMetaData().getMimeType();
		}
		ContentStream contentStream = createContentStream(doc.getDocKey(), doc.getDocContent(), mimeType);
		req.addContentStream(contentStream);
		return req;
	}

	public SolrServer getSolrServer() {
		return solrServer;
	}

	public void setSolrServer(SolrServer solrServer) {
		this.solrServer = solrServer;
	}


	public RFIRepository getSolrIndexRepo() {
		return solrIndexRepo;
	}

	public void setSolrIndexRepo(RFIRepository solrIndexRepo) {
		this.solrIndexRepo = solrIndexRepo;
	}



}
