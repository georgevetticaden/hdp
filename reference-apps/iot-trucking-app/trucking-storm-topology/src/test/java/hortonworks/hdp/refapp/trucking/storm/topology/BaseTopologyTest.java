package hortonworks.hdp.refapp.trucking.storm.topology;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;
import hortonworks.hdp.apputil.registry.RegistryKeys;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Before;
import org.junit.BeforeClass;



public class BaseTopologyTest {

		
	public static final String CONFIG_FILE_NAME = "trucking-streaming-hdp-service-config.properties";
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseTopologyTest.class);

	/**
	 * Creates the serviec Registry
	 * @return
	 * @throws Exception
	 */
	protected static HDPServiceRegistry createHDPServiceRegistry() throws Exception {
	
		// If System property is not set, then default to relative
		String serviceConfigDir = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		LOG.debug("The configured directory for HDP Service Registry is: " + serviceConfigDir);
		boolean isAbsolute = true;
		if(StringUtils.isEmpty(serviceConfigDir)) {
			serviceConfigDir = "/config/dev/registry";
			LOG.debug("No configured service director so usin the following: " + serviceConfigDir);
			isAbsolute = false;
		}
		
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceConfigDir, CONFIG_FILE_NAME, isAbsolute);
		serviceRegistry.populate();
		return serviceRegistry;
	}		
}
