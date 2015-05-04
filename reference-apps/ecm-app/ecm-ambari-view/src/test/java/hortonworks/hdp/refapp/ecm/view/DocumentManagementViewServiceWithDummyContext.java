package hortonworks.hdp.refapp.ecm.view;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.ambari.view.AmbariStreamProvider;
import org.apache.ambari.view.DataStore;
import org.apache.ambari.view.HttpImpersonator;
import org.apache.ambari.view.ImpersonatorSetting;
import org.apache.ambari.view.ResourceProvider;
import org.apache.ambari.view.SecurityException;
import org.apache.ambari.view.URLStreamProvider;
import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.ViewController;
import org.apache.ambari.view.ViewDefinition;
import org.apache.ambari.view.ViewInstanceDefinition;


public class DocumentManagementViewServiceWithDummyContext extends DocumentManagementViewService {
	
	private static final String configFileDir = "/config/dev";
	private static final String configName = "ecm-view-hdp-service-config.properties";

	protected void configureViewContext() {

		
		HDPServiceRegistry registry = populateRegistry();
		
		final Map<String, String> mockAmbariProperties = registry.getRegistry();
		
		ViewContext context = new ViewContext() {
			
			@Override
			public void removeInstanceData(String key) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void putInstanceData(String key, String value) {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void hasPermission(String userName, String permissionName)
					throws SecurityException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public String getViewName() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Collection<ViewInstanceDefinition> getViewInstanceDefinitions() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public ViewInstanceDefinition getViewInstanceDefinition() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Collection<ViewDefinition> getViewDefinitions() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public ViewDefinition getViewDefinition() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public String getUsername() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public URLStreamProvider getURLStreamProvider() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public ResourceProvider<?> getResourceProvider(String type) {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Map<String, String> getProperties() {
				return mockAmbariProperties;
			}
			
			@Override
			public String getInstanceName() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public String getInstanceData(String key) {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public Map<String, String> getInstanceData() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public ImpersonatorSetting getImpersonatorSetting() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public HttpImpersonator getHttpImpersonator() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public DataStore getDataStore() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public ViewController getController() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public AmbariStreamProvider getAmbariStreamProvider() {
				// TODO Auto-generated method stub
				return null;
			}
			
			@Override
			public String getAmbariProperty(String key) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		
		setContext(context);
		
	}

	private HDPServiceRegistry populateRegistry() {
		HDPServiceRegistry registry = new HDPServiceRegistryImpl(configFileDir, configName, false);
		
		try {
			registry.populate();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return registry;
	}		
}
