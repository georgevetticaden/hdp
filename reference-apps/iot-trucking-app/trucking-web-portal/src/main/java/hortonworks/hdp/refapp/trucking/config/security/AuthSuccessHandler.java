package hortonworks.hdp.refapp.trucking.config.security;


import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.refapp.trucking.config.registry.HDPRefAppServiceRegistryConfig;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthSuccessHandler extends SavedRequestAwareAuthenticationSuccessHandler {

	
	//private static final String SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY = "service.registry.config.location";
	private static final Logger LOG = LoggerFactory.getLogger(AuthSuccessHandler.class);
	
	
	public AuthSuccessHandler(String defaultSuccessUrl, boolean alwaysUse) {
		setDefaultTargetUrl(defaultSuccessUrl);
		setAlwaysUseDefaultTargetUrl(alwaysUse);
	}

	
	
	@Override
	public void onAuthenticationSuccess(HttpServletRequest request,
			HttpServletResponse arg1, Authentication auth) throws IOException,
			ServletException {
		
		super.onAuthenticationSuccess(request, arg1, auth);
		LOG.info("Invoking Authentication handler");
		
		String serviceConfigDir = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		String configFileName = auth.getName() + "-" + HDPRefAppServiceRegistryConfig.CONFIG_FILE_NAME;
		
		LOG.info("Attempting load user service registry via file["+configFileName);
		// Attemp to load service registry based on user
		HDPServiceRegistry registry = null;
		try{
			registry = new HDPServiceRegistryImpl(serviceConfigDir, configFileName, true);
			Map<String, String> updatedRegistryValues = registry.getRegistry();
			
			//If it is not empty, then populate the registry with the user values
			if(!updatedRegistryValues.isEmpty()) {
				try {
					request.getSession().setAttribute("HDP.REGISTRY", registry);
					
				} catch (Exception e) {
					String msg = "Error loading user registry for file["+configFileName + "]. Error is: " + e.getMessage();
					LOG.info(msg, e);
				}
			} else {
				String msg = "Attempted to load registry for user registry["+configFileName + "] but was empty. So using default registry";
				LOG.info(msg);
			}			
		} catch (Exception e) {
			String msg =  "Attempted to load registry for user registry["+configFileName + "] but doesn't exist. So using default registry";
			LOG.info(msg);
		}
		
		


	}	

}
