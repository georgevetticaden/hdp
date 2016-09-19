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
		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbari(DEFAULT_CONFIG_FILE_NAME, false);
	
		assertThat(registry.getRegistry().size(), is(23));
		assertThat(registry.getAmbariServerUrl(), is("http://hdp0.field.hortonworks.com:8080"));
		//assertThat(registry.getFalconServerPort(), is("15000"));
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
		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/hdp/app-utils/hdp-app-utils/src/test/resources/registry");
		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbari(DEFAULT_CONFIG_FILE_NAME, true);
	
		assertThat(registry.getRegistry().size(), is(23));
		assertThat(registry.getAmbariServerUrl(), is("http://hdp0.field.hortonworks.com:8080"));
		//assertThat(registry.getFalconServerPort(), is("15000"));
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
	public void testPopulateRegistryFromAmbari() throws Exception{

		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbari(DEFAULT_CONFIG_FILE_NAME, false);
		//do asserts
		testEntireRegistry(registry);
	}
	
	@Test
	public void testPopulateForHDFStack() throws Exception{

		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbariForHDFSTack(DEFAULT_CONFIG_FILE_NAME, false);
		//do asserts
		testHDFRegistry(registry);
	}
		
	
	
	@Test
	public void testWritingRegistryToFile() throws Exception {

		System.setProperty(RegistryKeys.SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY, "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/hdp/app-utils/hdp-app-utils/src/test/resources/registry");
		
		//Right now write is only supported with absolute Path
		HDPServiceRegistry registry = createHDPServiceRegistryWithAmbari("george-hdp-service-config.properties", true);
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
		
		//assertThat(serviceRegistry.getFalconServerUrl(),  is("http://centralregion03.cloud.hortonworks.com:15000"));
		
		assertThat(serviceRegistry.getHBaseZookeeperClientPort() ,  is("2181"));
		assertThat(serviceRegistry.getHBaseZookeeperHost(), is("hdp0.field.hortonworks.com"));
		//assertThat(serviceRegistry.getHBaseZookeeperZNodeParent(), is("/services/slider/users/yarn/hbase-on-yarn-v36"));
		
		assertThat(serviceRegistry.getHDFSUrl(), is("hdfs://hdp0.field.hortonworks.com:8020"));
		
		assertThat(serviceRegistry.getHiveMetaStoreUrl() , is("thrift://hdp2.field.hortonworks.com:9083"));
		assertThat(serviceRegistry.getHiveServer2ConnectionURL() , is("jdbc:hive2://hdp2.field.hortonworks.com:10000"));

		assertThat(serviceRegistry.getKafkaBrokerList()  , is("hdp0.field.hortonworks.com:6667"));
		assertThat(serviceRegistry.getKafkaZookeeperClientPort(), is("2181"));
		assertThat(serviceRegistry.getKafkaZookeeperHost(), is("hdp0.field.hortonworks.com"));
		assertThat(serviceRegistry.getKafkaZookeeperZNodeParent(), is(""));
		
		assertThat(serviceRegistry.getStormNimbusHost(), is("hdp1.field.hortonworks.com"));
		assertThat(serviceRegistry.getStormNimbusPort(), is("6627"));
		assertThat(serviceRegistry.getStormZookeeperQuorum(), is("hdp0.field.hortonworks.com,hdp1.field.hortonworks.com,hdp2.field.hortonworks.com"));
		assertThat(serviceRegistry.getStormUIUrl() , is("http://hdp1.field.hortonworks.com:8744"));

		
		List<String> zookeepers = serviceRegistry.getStormZookeeperQuorumAsList();
		assertThat(zookeepers.size(), is(3));
		for(String zookeeper: zookeepers) {
			System.out.println(zookeeper);
		}		

		assertThat(serviceRegistry.getPhoenixConnectionURL(), is("jdbc:phoenix:hdp0.field.hortonworks.com:2181:/hbase-unsecure"));

		assertThat(serviceRegistry.getClusterName(),  is("HDP_2_5"));
		
		assertThat(serviceRegistry.getAmbariServerUrl(),  is("http://hdp0.field.hortonworks.com:8080"));

		assertThat(serviceRegistry.getResourceManagerURL(), is("hdp1.field.hortonworks.com:8050"));
		assertThat(serviceRegistry.getResourceManagerUIURL() , is("http://hdp1.field.hortonworks.com:8088"));
		
		
		assertThat(serviceRegistry.getOozieUrl() , is("http://hdp2.field.hortonworks.com:11000/oozie"));	
	}
	
	public void testHDFRegistry(HDPServiceRegistry serviceRegistry) {
		
	
		assertThat(serviceRegistry.getClusterName(),  is("HDF_2_0_REF_APP"));
		
		assertThat(serviceRegistry.getAmbariServerUrl(),  is("http://hdf-ref-app0.field.hortonworks.com:8080/"));		
		assertThat(serviceRegistry.getKafkaBrokerList()  , is("hdf-ref-app6.field.hortonworks.com:6667,hdf-ref-app7.field.hortonworks.com:6667,hdf-ref-app8.field.hortonworks.com:6667"));
		assertThat(serviceRegistry.getKafkaZookeeperClientPort(), is("2181"));
		assertThat(serviceRegistry.getKafkaZookeeperHost(), is("hdf-ref-app2.field.hortonworks.com"));
		assertThat(serviceRegistry.getKafkaZookeeperZNodeParent(), is(""));
		
		assertThat(serviceRegistry.getStormNimbusHost(), is("hdf-ref-app2.field.hortonworks.com"));
		assertThat(serviceRegistry.getStormNimbusPort(), is("6627"));
		assertThat(serviceRegistry.getStormZookeeperQuorum(), is("hdf-ref-app0.field.hortonworks.com,hdf-ref-app1.field.hortonworks.com,hdf-ref-app2.field.hortonworks.com"));
		assertThat(serviceRegistry.getStormUIUrl() , is("http://hdf-ref-app2.field.hortonworks.com:8744"));

		
		List<String> zookeepers = serviceRegistry.getStormZookeeperQuorumAsList();
		assertThat(zookeepers.size(), is(3));
		for(String zookeeper: zookeepers) {
			System.out.println(zookeeper);
		}		

		


		assertThat(serviceRegistry.getHBaseZookeeperClientPort() ,  is("2181"));
		assertThat(serviceRegistry.getHBaseZookeeperHost(), is("hdp0.field.hortonworks.com"));
		assertThat(serviceRegistry.getHBaseZookeeperZNodeParent(), is("/hbase-unsecure"));
	
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
