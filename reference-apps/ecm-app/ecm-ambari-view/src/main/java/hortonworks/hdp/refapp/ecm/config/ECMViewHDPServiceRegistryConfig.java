package hortonworks.hdp.refapp.ecm.config;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ECMViewHDPServiceRegistryConfig {
	
	
	@Bean
	public HDPServiceRegistry serviceRegistry() throws Exception {
		return new HDPServiceRegistryImpl();
	}	
	

}
