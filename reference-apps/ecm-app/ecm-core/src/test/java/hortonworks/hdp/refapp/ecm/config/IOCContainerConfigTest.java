package hortonworks.hdp.refapp.ecm.config;

import static org.junit.Assert.assertNotNull;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.refapp.ecm.service.api.DocumentServiceAPI;
import hortonworks.hdp.refapp.ecm.service.config.DocumentServiceConfig;
import hortonworks.hdp.refapp.ecm.service.config.DocumentStoreConfig;
import hortonworks.hdp.refapp.ecm.service.config.IndexStoreConfig;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class IOCContainerConfigTest {


	@Test
	public void testCreatingIOCContainer() throws Exception {
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/config/dev/registry");
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_IS_ABSOLUTE_PROP_KEY, "false");
		
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(DocumentStoreConfig.class, IndexStoreConfig.class, DocumentServiceConfig.class, ECMCustomHDPServiceRegistryConfig.class);
		context.refresh();
		
		assertNotNull(context.getBean(DocumentStore.class));
		assertNotNull(context.getBean(IndexStore.class));
		assertNotNull(context.getBean(DocumentServiceAPI.class));
		
	}
	

}
