package hortonworks.hdp.apputil.registry;


import java.util.List;

import hortonworks.hdp.apputil.BaseUtilsTest;
import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtilsTest;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtilsTest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.util.StringUtils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class HDPServiceRegistryTest extends BaseUtilsTest {
	

	
	@Test
	public void testPopulateRegistryFromFileWithRelativePathFromCustomFileThatExists() throws Exception {
		
		//We are passing in a file that exists
		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams(DEFAULT_CONFIG_FILE_NAME, false);
	
		assertThat(registry.getRegistry().size(), is(24));
		assertThat(registry.getAmbariServerUrl(), is("http://centralregion01.cloud.hortonworks.com:8080"));
		assertThat(registry.getFalconServerPort(), is("15000"));
	}
	
	@Test
	public void testPopulateRegistryFromFileWithRelativePathFromCustomFileThatDoesntExist() throws Exception {
		
		//We are passing in a file that doesn't exists.
		
		try{
			HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams("file_doesnt_exist", false);
			Assert.fail("Exception should have been thrown for loading a file that doesn't exist");
		} catch (Exception e) {
			//as expected
		}
		
	
	}	
	
	@Test
	public void tesetPopulateRegistryFromFileWithRelativePathFromCustomFileAndDefaultFileThatDoesntExist() throws Exception {
		
		//Pass in a directory that doesn't exist so the file passed in or the default file is not found
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "directory_doesnt_exist");

		try {
			HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams(DEFAULT_CONFIG_FILE_NAME, false);
			Assert.fail("Should have filed since invalid directory for file was passed");
		} catch (Exception e) {
			//as expected
		}

	}		
	
	@Test
	public void tesetPopulateRegistryFromFileWithAbsolutePathWithFileThatExists() throws Exception {
		

		//this test will fail until you change to correct absolte path..
		//Pass in absolute file that exists
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/sedev/coe/hdp-app-utils/src/test/resources/registry");
		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams(DEFAULT_CONFIG_FILE_NAME, true);
	
		assertThat(registry.getRegistry().size(), is(24));
		assertThat(registry.getAmbariServerUrl(), is("http://centralregion01.cloud.hortonworks.com:8080"));
		assertThat(registry.getFalconServerPort(), is("15000"));
	}
	
//	@Test
//	public void tesetPopulateRegistryFromFileWithAbsolutePathWithFileThatDoesntExist() throws Exception {
//		
//
//		//this test will fail until you change to correct absolte path..
//		//pass in a absolute file that doesn't exist but it shoudl load the an absoulte default file called hdp-service-config.properties
//		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/sedev/coe/hdp-app-utils/src/test/resources/registry");
//		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams("file_doesnt_exist", true);
//	
//		assertThat(registry.getRegistry().size(), is(24));
//		assertThat(registry.getAmbariServerUrl(), is("http://centralregion01.cloud.hortonworks.com:8080"));
//		assertThat(registry.getFalconServerPort(), is("15000"));
//	}	
	
	@Test
	public void tesetPopulateRegistryFromFileWithAbsolutePathWithFileThatDoesntExistOrDefaultThatDoesntExist() throws Exception {
		

		//this test will fail until you change to correct absolte path..
		//pass in a directory that is not valid so the passed in file or default is not found
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "directory_doesnt_exist");
		try {
			HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams("file_doesnt_exist", true);
			Assert.fail("Exception shoudl have been thrown for invalid directory");
		} catch (Exception e) {
			//as expected
		}
		
	}	
	
	@Test
	public void testPopulateRegistryFromAmbariAndSliderHBaseAndStorm() throws Exception{

		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams(DEFAULT_CONFIG_FILE_NAME, false);
		//do asserts
		testEntireRegistry(registry);
	}
	
	@Test
	public void testWritingRegistryToFile() throws Exception {

		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/sedev/coe/hdp-app-utils/src/test/resources/registry");
		
		//Right now write is only supported with absolute Path
		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariAndSliderParams("written-to-hdp-service-config.properties", true);
		registry.writeToPropertiesFile();
	}
	
	@Test
	public void testPopulateRegistryWithNoDataInFileAndEmptyRegistryParams() throws Exception {
		String serviceRegistryPropertyFileLocation = getConfigDirectoryLocation();
				
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceRegistryPropertyFileLocation, "empty-hdp-service-config.properties", false);
		//pass in null for params
		serviceRegistry.populate(null);
		
		//registry should be empty
		assertThat(serviceRegistry.getRegistry().size(), is(0));
		
		//pass in empty params
		ServiceRegistryParams params = new ServiceRegistryParams();
		serviceRegistry.populate(params);
		
		//registry should be empty
		assertThat(serviceRegistry.getRegistry().size(), is(0));		
		
	}


	
	@Test
	public void testPopulateRegistryWithConfigFileWithAmbariInfo() throws Exception {
		String serviceRegistryPropertyFileLocation = getConfigDirectoryLocation();
				
		// pass in a config file with Ambari URL and cluter and hbase deployment info. This will allow you to call populate
		// which populates REgistry from Ambari and Slider.
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceRegistryPropertyFileLocation, "self-populating-hdp-service-config.properties", false);
		serviceRegistry.populate();
		
		//do asserts
		assertThat(serviceRegistry.getRegistry().size(), is(28));
		testEntireRegistry(serviceRegistry);		
	}
	
	@Test
	public void testPopulateRegistryWithCustomValue() {
		String serviceRegistryPropertyFileLocation = getConfigDirectoryLocation();
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceRegistryPropertyFileLocation, "custom-hdp-service-config.properties", false);
		
		assertThat(serviceRegistry.getRegistry().size(), is(1));
		assertThat(serviceRegistry.getCustomValue("solr.server.url"), is("http://vett-search01.cloud.hortonworks.com:8983/solr"));
		
	}
	
	public void testEntireRegistry(HDPServiceRegistry serviceRegistry) {
		
		assertThat(serviceRegistry.getFalconServerUrl(),  is("http://centralregion03.cloud.hortonworks.com:15000"));
		
		assertThat(serviceRegistry.getHBaseZookeeperClientPort() ,  is("2181"));
		assertThat(serviceRegistry.getHBaseZookeeperHost(), is("centralregion01.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getHBaseZookeeperZNodeParent(), is("/services/slider/users/yarn/hbase-on-yarn-v36"));
		
		assertThat(serviceRegistry.getHDFSUrl(), is("hdfs://centralregion01.cloud.hortonworks.com:8020"));
		
		assertThat(serviceRegistry.getHiveMetaStoreUrl() , is("thrift://centralregion03.cloud.hortonworks.com:9083"));
		assertThat(serviceRegistry.getHiveServer2ConnectionURL() , is("jdbc:hive2://centralregion03.cloud.hortonworks.com:10000"));

		assertThat(serviceRegistry.getKafkaBrokerList()  , is("centralregion01.cloud.hortonworks.com:6667,centralregion02.cloud.hortonworks.com:6667"));
		assertThat(serviceRegistry.getKafkaZookeeperClientPort(), is("2181"));
		assertThat(serviceRegistry.getKafkaZookeeperHost(), is("centralregion01.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getKafkaZookeeperZNodeParent(), is(""));
		
		assertThat(serviceRegistry.getStormNimbusHost(), is("centralregion08.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getStormNimbusPort(), is("46464"));
		assertThat(serviceRegistry.getStormZookeeperQuorum(), is("centralregion01.cloud.hortonworks.com,centralregion02.cloud.hortonworks.com,centralregion03.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getStormUIUrl() , is("http://centralregion09.cloud.hortonworks.com:57725"));

		
		List<String> zookeepers = serviceRegistry.getStormZookeeperQuorumAsList();
		assertThat(zookeepers.size(), is(3));
		for(String zookeeper: zookeepers) {
			System.out.println(zookeeper);
		}		

		assertThat(serviceRegistry.getPhoenixConnectionURL(), is("jdbc:phoenix:centralregion01.cloud.hortonworks.com:2181:/services/slider/users/yarn/hbase-on-yarn-v36"));

		assertThat(serviceRegistry.getClusterName(),  is("centralregioncluster"));
		
		assertThat(serviceRegistry.getAmbariServerUrl(),  is("http://centralregion01.cloud.hortonworks.com:8080"));

		assertThat(serviceRegistry.getResourceManagerURL(), is("centralregion02.cloud.hortonworks.com:8050"));
		assertThat(serviceRegistry.getResourceManagerUIURL() , is("http://centralregion02.cloud.hortonworks.com:8088"));
		
		
		assertThat(serviceRegistry.getOozieUrl() , is("http://centralregion03.cloud.hortonworks.com:11000/oozie"));	
	}
	
	@Test
	public void testHDPServiceRegistryWithEmptyConstructorAndThenPopulate() throws Exception {
		
		HDPServiceRegistry sourceRegistry = createHDPServiceRegistryWithAmbariAndSliderParams(DEFAULT_CONFIG_FILE_NAME, false);
		
		HDPServiceRegistry destinationRegistry = new HDPServiceRegistryImpl();
		
		destinationRegistry.populate(null, sourceRegistry.getRegistry(), null);
		testEntireRegistry(destinationRegistry);	
		
	}
	
//	private HDPServiceRegistry createCustomRegistry() throws Exception {
//		
//		String serviceRegistryPropertyFileLocation = System.getProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY);
//		if(StringUtils.isEmpty(serviceRegistryPropertyFileLocation)) {
//			throw new RuntimeException("To run this Test, you need to configured a system property called["+ RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY + "] " 
//					+ "that points to he location where your registry config directory is located[e.g: /Users/gvetticaden/Dropbox/Hortonworks/Development/Git/sedev/coe/hdp-app-utils/src/test/resources/registry]");
//		}
//		String customRegistryLocation = serviceRegistryPropertyFileLocation + "/custom";
//		
//		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(customRegistryLocation, CONFIG_FILE_NAME);
//		serviceRegistry.populate(createEmptyServiceRegistryParams());
//		return serviceRegistry;
//	}	
	
	
	
	
}
