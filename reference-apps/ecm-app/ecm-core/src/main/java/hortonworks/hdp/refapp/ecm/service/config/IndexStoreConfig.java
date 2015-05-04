package hortonworks.hdp.refapp.ecm.service.config;


import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.registry.ECMRegistryKeys;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.SolrIndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.repository.RFIRepository;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.solr.repository.support.SolrRepositoryFactory;

@Configuration
public class IndexStoreConfig {

	@Autowired
	HDPServiceRegistry serviceRegistry;
	
	@Bean
	public IndexStore indexStore() {
		return new SolrIndexStore(serviceRegistry);
	}	
	
	
}
