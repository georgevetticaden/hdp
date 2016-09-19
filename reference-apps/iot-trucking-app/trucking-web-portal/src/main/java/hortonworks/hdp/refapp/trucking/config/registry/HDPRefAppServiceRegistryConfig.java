package hortonworks.hdp.refapp.trucking.config.registry;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;
import hortonworks.hdp.apputil.registry.RegistryKeys;

import org.apache.commons.lang.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.web.context.WebApplicationContext;

@Configuration
public class HDPRefAppServiceRegistryConfig {
	
	public static final String CONFIG_FILE_NAME = "ref-app-hdp-service-config.properties";


	/**
	 * The service registry bean is session scoped since each logged in user might load up a seperate registry
	 */
	@Bean
	@Scope(value=WebApplicationContext.SCOPE_SESSION, proxyMode=ScopedProxyMode.TARGET_CLASS)
	public HDPServiceRegistry serviceRegistry() throws Exception {
		String serviceConfigDir = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		
		boolean absDir = true;
		String absDirString = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_IS_ABSOLUTE_PROP_KEY);
		if(StringUtils.isNotEmpty(absDirString)) {
			absDir = Boolean.valueOf(absDirString);
		}
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceConfigDir, CONFIG_FILE_NAME, absDir);
		serviceRegistry.populateForHDF();;
		return serviceRegistry;
	}	

}


