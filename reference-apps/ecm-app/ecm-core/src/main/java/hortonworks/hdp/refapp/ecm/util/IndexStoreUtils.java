package hortonworks.hdp.refapp.ecm.util;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.solr.repository.support.SolrRepositoryFactory;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.registry.ECMRegistryKeys;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIRepository;

public final class IndexStoreUtils {

	public static void truncate(HDPServiceRegistry serviceRegistry) {
		String url = serviceRegistry.getCustomValue(ECMRegistryKeys.SOLR_SERVER_URL) + "/" + serviceRegistry.getCustomValue(ECMRegistryKeys.ECM_SOLR_CORE);
	    SolrServer solrServer = new HttpSolrServer(url);
	    RepositoryFactorySupport solrSupport = new SolrRepositoryFactory(solrServer);
	    RFIRepository solrIndexRepo = solrSupport.getRepository(RFIRepository.class);		
	    
	    solrIndexRepo.deleteAll();
	}
}
