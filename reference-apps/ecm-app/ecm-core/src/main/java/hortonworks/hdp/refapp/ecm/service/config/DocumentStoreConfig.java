package hortonworks.hdp.refapp.ecm.service.config;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.docstore.HBaseDocumentStore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DocumentStoreConfig {
	
	@Autowired
	HDPServiceRegistry registry;

	@Bean
	public DocumentStore docStore() {
		return new HBaseDocumentStore(registry);
	}	
	
}
