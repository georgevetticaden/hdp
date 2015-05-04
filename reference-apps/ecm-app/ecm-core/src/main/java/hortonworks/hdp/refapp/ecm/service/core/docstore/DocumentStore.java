package hortonworks.hdp.refapp.ecm.service.core.docstore;

import java.io.Serializable;
import java.util.List;

import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;


public interface DocumentStore extends Serializable {
	
	/**
	 * Stores the Document Blob in the Document Store. Currently No Metadata is stored in the Store
	 * @param docUpdateRequest
	 * @throws Exception
	 */
	void storeDocument(DocumentDetails docDetails) throws Exception;
	
	/**
	 * Retrieves the Document blob via the Key
	 * @param key
	 * @return
	 * @throws Exception
	 */
	DocumentDetails getDocument(String key) throws Exception;
	
	
	/**
	 * Returns all of the document keys in the Document Store
	 * @return
	 * @throws Exception
	 */
	List<String> getAllDocumentKeys() throws Exception;
	


}
