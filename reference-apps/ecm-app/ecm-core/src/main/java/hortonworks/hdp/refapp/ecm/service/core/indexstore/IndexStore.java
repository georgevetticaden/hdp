package hortonworks.hdp.refapp.ecm.service.core.indexstore;

import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentIndexDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentSearchRequest;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.QueryResult;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIDocument;

import java.io.InputStream;
import java.util.List;

public interface IndexStore {

	
	/**
	 * Adds the Document BLob and the corresponding Metadata to the Index Store
	 * @param docUpdateRequest
	 * @throws Exception
	 */
	void addDocument(DocumentIndexDetails indexDetails) throws Exception;
	
	/**
	 * Removes the Document and the corresponding Metadata fro the Index Store
	 * @param id
	 * @throws Exception
	 */
	void removeDocument(String id) throws Exception;
	
	/**
	 * Gets the Document from the Index Store via the primary key
	 * @param id
	 * @return
	 * @throws Exception
	 */
	RFIDocument getDocument(String id) throws Exception;
	
	
	/**
	 * Search Document via any search Value and return all results
	 * @param searchValue
	 * @return
	 * @throws Exception
	 */
	List<RFIDocument> searchByBody(String searchValue) throws Exception;
	
	
	/**
	 * Search Documents with more complicated Request Param
	 * @param request
	 * @return
	 * @throws Exception
	 */
	QueryResult searchAll(DocumentSearchRequest request) throws Exception;
	
	
	/**
	 * Search Documents via Customer Name
	 * @param name
	 * @return
	 * @throws Exception
	 */
	List<RFIDocument> searchByCustomerName(String name) throws Exception;
	

	/**
	 * Returns all of the document keys in the Index Store
	 * @return
	 * @throws Exception
	 */
	List<String> getAllDocumentKeys() throws Exception;
	
	

}
