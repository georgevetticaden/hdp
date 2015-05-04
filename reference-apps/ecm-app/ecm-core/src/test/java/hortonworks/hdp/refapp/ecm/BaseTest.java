package hortonworks.hdp.refapp.ecm;


import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.refapp.ecm.config.ECMCustomHDPServiceRegistryConfig;
import hortonworks.hdp.refapp.ecm.service.config.DocumentServiceConfig;
import hortonworks.hdp.refapp.ecm.service.config.DocumentStoreConfig;
import hortonworks.hdp.refapp.ecm.service.config.IndexStoreConfig;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentIndexDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.IndexMetaData;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

@ContextConfiguration(classes={IndexStoreConfig.class, DocumentStoreConfig.class, DocumentServiceConfig.class,  ECMCustomHDPServiceRegistryConfig.class}) 
public abstract class BaseTest extends AbstractJUnit4SpringContextTests {

	/**
	 * Default to relative path and local config if the property has not been set as part of -D setting
	 */
	@BeforeClass
	public static void setUpSystemRegistryConfigDirectoryLocation() {
		String serviceConfigDir = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		
		if(StringUtils.isEmpty(serviceConfigDir)) {
			System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/config/dev/registry");
			System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_IS_ABSOLUTE_PROP_KEY, "false");
		}

	}
	
	

	
	protected byte[] getTestDocument(String fileName) throws Exception {
		InputStream document = this.getClass().getClassLoader().getResourceAsStream(fileName);
		return IOUtils.toByteArray(document);
	}	

	protected DocumentIndexDetails createDocIndexDetails(String docId, byte[] docContent, IndexMetaData indexMetadata) {
		DocumentIndexDetails docIndexDetails = new DocumentIndexDetails(docId, docContent, indexMetadata);
		return docIndexDetails;
	}	

	
		
}
