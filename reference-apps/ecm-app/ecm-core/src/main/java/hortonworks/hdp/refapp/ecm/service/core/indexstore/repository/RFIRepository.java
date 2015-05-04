package hortonworks.hdp.refapp.ecm.service.core.indexstore.repository;

import java.util.List;

import org.springframework.data.solr.repository.SolrCrudRepository;

public interface RFIRepository extends SolrCrudRepository<RFIDocument, String>{
	
	List<RFIDocument> findByBody(String body);

	RFIDocument findById(String id);

	List<RFIDocument> findByCustomername(String customer);
	

}
