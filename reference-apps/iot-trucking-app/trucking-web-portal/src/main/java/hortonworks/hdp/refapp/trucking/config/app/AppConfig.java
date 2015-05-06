package hortonworks.hdp.refapp.trucking.config.app;

import java.io.IOException;
import java.util.Properties;

import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.VelocityException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.ui.velocity.VelocityEngineFactoryBean;

@Configuration
//@PropertySource("classpath:/hdp-refapp-config.properties")
public class AppConfig {

	 	@Bean
	   public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
	      return new PropertySourcesPlaceholderConfigurer();
	   }	
	 	
	 	@Bean
	 	public VelocityEngine velocityEngine() throws VelocityException, IOException{
	 	    VelocityEngineFactoryBean factory = new VelocityEngineFactoryBean();
	 	    Properties props = new Properties();
	 	    props.put("resource.loader", "class");
	 	    props.put("class.resource.loader.class",
	 	              "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
	 	    factory.setVelocityProperties(props);
	 	    
	 	    return factory.createVelocityEngine();
	 	}		 	
	 	
//	 	
//		/* Must be supplied as System Property or config properties */
//		private @Value("${activemq.host}") String activeMQHost;
//		
//
//
//		public String getActiveMQHost() {
//			return activeMQHost;
//		}
		


	 	
}
