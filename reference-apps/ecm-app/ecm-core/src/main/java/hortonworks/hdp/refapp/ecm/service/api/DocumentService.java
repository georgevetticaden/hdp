package hortonworks.hdp.refapp.ecm.service.api;

import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentProperties;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentIndexDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentSearchRequest;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.IndexMetaData;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.QueryResult;
import hortonworks.hdp.refapp.ecm.util.DocumentGUIDUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class DocumentService implements DocumentServiceAPI {
	

	private static final Logger LOG = Logger.getLogger(DocumentService.class);
	
	
	private DocumentStore docStore;
	private IndexStore indexStore;
	
	public DocumentService(DocumentStore docStore, IndexStore indexStore) {
		super();
		this.docStore = docStore;
		this.indexStore = indexStore;
	}

	@Override
	public String addDocument(DocumentUpdateRequest docUpdateRequest) throws Exception {
		
		
		validate(docUpdateRequest);
		// Generate key for new document if key is not passed
		String docKey = docUpdateRequest.getDocKey();
		if(StringUtils.isEmpty(docKey))  {
			docKey = DocumentGUIDUtils.generateDocGUID();
			LOG.info("Generating New Doc Key["+ docKey + "]");
		}
			
		
		
		//Store Document into Document store
		if(LOG.isInfoEnabled()) {
			LOG.info("Inserting Document with Key["+docKey+"] into Document Store");
		}
		
		DocumentProperties docProperties = constructDocProperties(docUpdateRequest.getDocMetadata());
		DocumentDetails docDetails = constructDocDetails(docUpdateRequest, docKey, docProperties);
		docStore.storeDocument(docDetails);
		
		if(LOG.isInfoEnabled()) {
			LOG.info("Successfully Insert Document with Key["+docKey+"] into Document Store");
		}
		
		
		//Store the Index into IndexStore
		if(LOG.isInfoEnabled()) {
			LOG.info("Inserting Index with Key["+docKey+"] into Index Store");
		}
		
		IndexMetaData indexMetaData = constructIndexMetaData(docUpdateRequest.getDocMetadata());
		DocumentIndexDetails docIndexDetails = constructDocIndexDetails(docUpdateRequest, docKey, indexMetaData);
		indexStore.addDocument(docIndexDetails);
		
		if(LOG.isInfoEnabled()) {
			LOG.info("Successfully Insert Index with Key["+docKey+"] into Index Store");
		}
		
		
		return docKey;

	}


	
	private void validate(DocumentUpdateRequest docUpdateRequest) {
		if(docUpdateRequest == null || docUpdateRequest.getDocMetadata() == null) {
			String errMsg = "DocUupdateRequest and DocMetadata must be populated";
			LOG.error(errMsg);
			throw new RuntimeException(errMsg);
		}
		
	}



	private DocumentIndexDetails constructDocIndexDetails(
			DocumentUpdateRequest docUpdateRequest, String docKey,
			IndexMetaData indexMetaData) {
		// TODO Auto-generated method stub
		return new DocumentIndexDetails(docKey, docUpdateRequest.getDocumentContent(), indexMetaData);
	}



	private IndexMetaData constructIndexMetaData(DocumentMetaData docMetadata) {
		IndexMetaData indexMetaData = new IndexMetaData(docMetadata.getDocumentName(), docMetadata.getMimeType(), docMetadata.getDocClassType(), docMetadata.getCustomerName());
		return indexMetaData;
	}


	public DocumentDetails getDocument(String key) throws Exception {
		return docStore.getDocument(key);
	}
	

	@Override
	public QueryResult searchDocuments(DocumentSearchRequest docSearchRequest)
			throws Exception {
		
		return indexStore.searchAll(docSearchRequest);
	}

	private DocumentDetails constructDocDetails(DocumentUpdateRequest docUpdateRequest,
			String docKey, DocumentProperties docProperties) {
		return new DocumentDetails(docKey, docUpdateRequest.getDocumentContent(), docProperties);
	}



	private DocumentProperties constructDocProperties(
			DocumentMetaData docMetaData) {
		DocumentProperties docProperties = null;
		if(docMetaData != null) {
			docProperties = new DocumentProperties(docMetaData.getDocumentName(), docMetaData.getMimeType(), docMetaData.getExtension());
		}
		return docProperties;
	}

	
}
