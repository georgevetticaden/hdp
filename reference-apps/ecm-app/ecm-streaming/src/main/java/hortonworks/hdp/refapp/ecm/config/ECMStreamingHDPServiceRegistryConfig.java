package hortonworks.hdp.refapp.ecm.config;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ECMStreamingHDPServiceRegistryConfig {
	
	private static final String CONFIG_FILE_NAME = "ecm-view-hdp-service-config.properties";
	public static final String SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY = "service.registry.config.location";
	

	@Bean
	public HDPServiceRegistry serviceRegistry() throws Exception {
		return new HDPServiceRegistryImpl();
	}	
	

}
