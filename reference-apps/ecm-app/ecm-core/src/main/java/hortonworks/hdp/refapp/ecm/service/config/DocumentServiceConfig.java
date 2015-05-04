package hortonworks.hdp.refapp.ecm.service.config;

import hortonworks.hdp.refapp.ecm.service.api.DocumentService;
import hortonworks.hdp.refapp.ecm.service.api.DocumentServiceAPI;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DocumentServiceConfig {
	
	@Autowired
	DocumentStore docStore;
	
	@Autowired
	IndexStore indexStore;

	@Bean
	public DocumentServiceAPI docServiceConfig() {
		return new DocumentService(docStore, indexStore);
	}	

}
