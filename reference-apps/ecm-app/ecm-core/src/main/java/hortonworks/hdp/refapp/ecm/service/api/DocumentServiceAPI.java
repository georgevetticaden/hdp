package hortonworks.hdp.refapp.ecm.service.api;

import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentSearchRequest;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.QueryResult;


public interface DocumentServiceAPI {

	/**
	 * Adds the Document to the Document and Index Store
	 * @param docUpdateRequest
	 * @return
	 * @throws Exception
	 */
	String addDocument(DocumentUpdateRequest docUpdateRequest) throws Exception;

	/**
	 * Returns the Document and document properties from the document Store
	 * @param key
	 * @return
	 * @throws Exception
	 */
	DocumentDetails getDocument(String key) throws Exception;
	
	/**
	 * Searches for Documents via the Index Store
	 * @param searchRequest
	 * @return
	 * @throws Exception
	 */
	QueryResult searchDocuments(DocumentSearchRequest searchRequest) throws Exception;

	
}
