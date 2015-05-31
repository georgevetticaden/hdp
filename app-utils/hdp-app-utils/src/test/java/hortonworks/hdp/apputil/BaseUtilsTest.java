package hortonworks.hdp.apputil;

import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.util.StringUtils;

import hortonworks.hdp.apputil.registry.DeploymentMode;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.apputil.registry.ServiceRegistryParams;
import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtilsTest;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtilsTest;

public abstract class BaseUtilsTest {

	
	@Before
	public void setUpSystemRegistryConfigDirectoryLocation() {
		//default to relative path
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/registry");
	}
	
	public String getConfigDirectoryLocation() {
		String serviceRegistryPropertyFileLocation = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		return serviceRegistryPropertyFileLocation;
	}		
	
	public static final String DEFAULT_CONFIG_FILE_NAME = "hdp-service-config.properties";	
	private static final String AMBARI_SERVER_URL = "http://centralregion01.cloud.hortonworks.com:8080";

	
	
	protected HDPServiceRegistry createHDPServiceRegistryWithAmbariAndSliderParams(String configFileName, boolean isAbsolutePath) throws Exception {
		
		String serviceRegistryPropertyFileLocation = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		if(StringUtils.isEmpty(serviceRegistryPropertyFileLocation)) {
			throw new RuntimeException("To run this Test, you need to configured a system property called["+ RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY + "] " 
					+ "that points to he location where your registry config directory is located[e.g: /Users/gvetticaden/Dropbox/Hortonworks/Development/Git/sedev/coe/hdp-app-utils/src/test/resources/registry]");
		}
		
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceRegistryPropertyFileLocation, configFileName, isAbsolutePath);
		serviceRegistry.populate(createServiceRegistryParamsWithAmbariAndSlider());
		return serviceRegistry;
	}	
	
	
	protected HDPServiceRegistry createHDPServiceRegistryWithEmptyParams(String configFileName, boolean isAbsolutePath) throws Exception {
		
		String serviceRegistryPropertyFileLocation = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
		if(StringUtils.isEmpty(serviceRegistryPropertyFileLocation)) {
			throw new RuntimeException("To run this Test, you need to configured a system property called["+ RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY + "] " 
					+ "that points to he location where your registry config directory is located[e.g: /Users/gvetticaden/Dropbox/Hortonworks/Development/Git/sedev/coe/hdp-app-utils/src/test/resources/registry]");
		}
		
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceRegistryPropertyFileLocation, configFileName, isAbsolutePath);
		serviceRegistry.populate(createEmptyServiceRegistryParams());
		return serviceRegistry;
	}	
	
	
	private ServiceRegistryParams createServiceRegistryParamsWithAmbariAndSlider() {
		ServiceRegistryParams params = new ServiceRegistryParams();
		params.setAmbariUrl(AMBARI_SERVER_URL);
		params.setClusterName("centralregioncluster");
		
		params.setStormDeploymentMode(DeploymentMode.SLIDER);
		params.setStormSliderPublisherUrl(StormSliderUtilsTest.SLIDER_STORM_PUBLISHER_URL);
		
		params.setHbaseDeploymentMode(DeploymentMode.SLIDER);
		params.setHbaseSliderPublisherUrl(HBaseSliderUtilsTest.SLIDER_HBASE_PUBLISHER_URL);
		
		return params;
	}	
	
	private ServiceRegistryParams createEmptyServiceRegistryParams() {
		ServiceRegistryParams params = new ServiceRegistryParams();		
		return params;
	}		
}