package hortonworks.hdp.refapp.ecm.config;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;
import hortonworks.hdp.apputil.registry.RegistryKeys;

import org.apache.commons.lang.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ECMCustomHDPServiceRegistryConfig {
	
	private static final String CONFIG_FILE_NAME = "ecm-hdp-service-config.properties";


	@Bean
	public HDPServiceRegistry serviceRegistry() throws Exception {
				
		String serviceConfigDir = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		
		boolean absDir = true;
		String absDirString = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_IS_ABSOLUTE_PROP_KEY);
		if(StringUtils.isNotEmpty(absDirString)) {
			absDir = Boolean.valueOf(absDirString);
		}
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceConfigDir, CONFIG_FILE_NAME, absDir);
		serviceRegistry.populate();
		return serviceRegistry;
	}	
	

}
