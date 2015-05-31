package hortonworks.hdp.apputil.registry;

import hortonworks.hdp.apputil.ambari.AmbariUtils;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtils;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * Implementation of Service Registry. Discovers endpoint configuration via the following:
 * 	1. Via a properties file that you pass in via the configFileLocation and configName
 *  2. Via Ambari REST if you pass in an Ambari URL and cluster name
 *  3. Slider REST if you pass in the appropriate Publisher URLS (Hbase or Storm Slider Publisher Urls)
 * @author gvetticaden
 *
 */
public class HDPServiceRegistryImpl implements HDPServiceRegistry{

	private static final Logger LOG = Logger.getLogger(HDPServiceRegistryImpl.class);
	//private static final String DEFAULT_CONFIG_NAME = "hdp-service-config.properties";
	
	private Map<String, String> registry = new HashMap<String, String>();
	private String hdpServiceRegistryConfigLocation;
	private String configFileName;
	
	
	/**
	 * 
	 * @param configFileLocation - Location where the properties files is located
	 * @param configName - The name of the config file. If it can't find it. It will use a default properties called hdp-service-config.properties
	 * @param isAbsolutePath - Signifies that the configFileLocation is an absolute or relative path
	 */
	public HDPServiceRegistryImpl(String configFileLocation, String configName, boolean isAbsolutePath) {
		this.hdpServiceRegistryConfigLocation = configFileLocation;
		this.configFileName = configName;
		this.populateRegistryFromConfigFile(isAbsolutePath);
	}
	
	/**
	 * Empty constructor when you don't to populate registry from config file;
	 */
	public HDPServiceRegistryImpl() {
		
	}
	
	
	private ServiceRegistryParams constructServiceRegistryParamsFromConfigFile() {
		ServiceRegistryParams params = new ServiceRegistryParams();
		params.setAmbariUrl(getAmbariServerUrl());
		params.setClusterName(getClusterName());
		if(StringUtils.isNotEmpty(getHBaseDeploymentMode())) {
			params.setHbaseDeploymentMode(DeploymentMode.valueOf(getHBaseDeploymentMode()));
		}
		if(StringUtils.isNotEmpty(getStormDeploymentMode())) {
			params.setStormDeploymentMode(DeploymentMode.valueOf(getStormDeploymentMode()));
		}
		
		params.setHbaseSliderPublisherUrl(getHBaseSliderPublisherURL());
		params.setStormSliderPublisherUrl(getStormSliderPublisherURL());
		return params;
	}


	/**
	 * Populates the registry further based on the Service Registry Params which can:
	 * 	1. Load further endpoints from Ambari REST
	 *  2. Load endpoints from Slider
	 */
	public void populate(ServiceRegistryParams params, Map<String, String> customParams, String newConfigFileName) throws Exception {
		
		if(params != null) {
			
			if(StringUtils.isNotEmpty(params.getAmbariUrl()) || StringUtils.isNotEmpty(params.getClusterName())) {
				saveToRegistry(RegistryKeys.AMBARI_SERVER_URL, params.getAmbariUrl());
				saveToRegistry(RegistryKeys.AMBARI_CLUSTER_NAME, params.getClusterName());
				
				String ambariRestUrl = constructAmbariRestURL(params.getAmbariUrl(), params.getClusterName());
				
				AmbariUtils ambariService = new AmbariUtils(ambariRestUrl);
				populateRegistryEndpointsFromAmbari(ambariService);
				
				populateRegistryEndpointsForHBase(ambariService, params);
				populateRegistryEndpointsForStorm(ambariService, params);
								
			} else {
				LOG.info("Skipping populating registry from Ambari because no Ambari Server URL or clusterName was not provided");
				populateRegistryEndpointsForHBase(params);
				populateRegistryEndpointsForStorm(params);
			}
		} else {
			LOG.info("Service Registry Params is null. So skippign populating from Ambari and Slider");
		}
		
		if(customParams!= null) {
			LOG.info("Populating Custom Params");
			for(String key: customParams.keySet()) {
				saveToRegistry(key, customParams.get(key));
			}
		}
		
		if(StringUtils.isNotEmpty(newConfigFileName)) {
			this.configFileName = newConfigFileName;
		}
				
	}	
	
	@Override
	public void populate(ServiceRegistryParams params) throws Exception {
		populate(params, null, null);
		
	}		
	
	/**
	 * Populate the registry by constructing the registry params based on initial config file
	 * @throws Exception
	 */
	public void populate() throws Exception {
		ServiceRegistryParams serviceRegistryParams = constructServiceRegistryParamsFromConfigFile();
		populate(serviceRegistryParams);
	}
	

	/**
	 * Writes the Registry to the config file.
	 * Use it if you want to persistent it to avoid future REST calls
	 */
	public void writeToPropertiesFile() throws Exception {
		writeToPropertiesFile(this.configFileName);
	}	
	
	/**
	 * Writes the Registry to the config file.
	 * Use it if you want to persistent it to avoid future REST calls
	 */
	public void writeToPropertiesFile(String fileName) throws Exception {
		Map<String, String> registry = getRegistry();
		
		Properties properties = new Properties();
		for(String key: registry.keySet()) {
			String value = registry.get(key);
			if(value != null)
				properties.put(key, value);
		}
		String fullFileName = constructFullFileName(this.hdpServiceRegistryConfigLocation,fileName);
		FileOutputStream out = new FileOutputStream(fullFileName);
		properties.store(out, null);
		LOG.info("Finished writing new properties file ["+ fullFileName + "]" );
	}		
	
	/**
	 * Populates the registry from the config file
	 * @param isAbsolutePath
	 */
	private void populateRegistryFromConfigFile(boolean isAbsolutePath)   {
		Properties hdpServiceConfigProperties = null;
		InputStream inputStream = null;
		try {
			hdpServiceConfigProperties = new Properties();
			inputStream = createConfigInputStream(isAbsolutePath);
			hdpServiceConfigProperties.load(inputStream);
			LOG.info("Initial Load from Config File is: " + hdpServiceConfigProperties);
		} catch (Exception e) {
			String errorMsg = "Encountered error while reading configuration properties: "
					+ e.getMessage();
			LOG.error(errorMsg);
			throw new RuntimeException(errorMsg, e);
		} finally {
			if(inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					LOG.error("Error closing inputStream");
				}
			}
		}
		Set<Object> keys = hdpServiceConfigProperties.keySet();
		for(Object propertyKey: keys) {
			String key = (String)propertyKey;
			String value = hdpServiceConfigProperties.getProperty(key);
			saveToRegistry(key, value);
		}
	}


	


	private void populateRegistryEndpointsFromAmbari(AmbariUtils ambariService) throws Exception {
		LOG.info("Starting to populate Service Registry from Ambari");
		
		//falcon configuration
		String falconSeverUrl = "http://"+ambariService.getFalconHost() + ":" + getValueForKey(RegistryKeys.FALCON_SERVER_PORT);
		saveToRegistry(RegistryKeys.FALCON_SERVER_URL , falconSeverUrl);
		saveToRegistry(RegistryKeys.FALCON_BROKER_URL , ambariService.getFalconBrokerUrl());
		
		//HDFS configuration
		saveToRegistry(RegistryKeys.HDFS_URL , ambariService.getHDFSUrl());

		//YARN configuration
		saveToRegistry(RegistryKeys.RESOURCE_MANGER_URL, ambariService.getResourceManagerUrl());
		saveToRegistry(RegistryKeys.RESOURCE_MANGER_URI_URL, ambariService.getResourceManagerUIUrl());
		
		
		//Hive Configuration
		saveToRegistry(RegistryKeys.HIVE_METASTORE_URL, ambariService.getHiveMetaStoreUrl());
		String hiveServer2ConnectString = "jdbc:hive2://"+ambariService.getHiveServer2Host() +":"+ambariService.getHiveServer2ThriftPort();
		saveToRegistry(RegistryKeys.HIVE_SERVER2_CONNECT_URL, hiveServer2ConnectString);
		
	
		//Oozie Configuration
		saveToRegistry(RegistryKeys.OOZIE_SERVER_URL, ambariService.getOozieServerUrl());
		

		//Kafka Configuration
		
		List<String> kafkaBrokerHosts = ambariService.getKafkaBrokerList();
		String kafkaBrokerPort = ambariService.getKafkaBrokerPort();
		StringBuffer kafkaBrokerListBuffer = new StringBuffer();
		boolean isFirst = true;
		for(String kafkaBrokerHost: kafkaBrokerHosts) {
			if(!isFirst) {
				kafkaBrokerListBuffer.append(",");
			}
			kafkaBrokerListBuffer.append(kafkaBrokerHost).append(":").append(kafkaBrokerPort);
			isFirst = false;
		}
		
		saveToRegistry(RegistryKeys.KAFKA_BROKER_LIST, kafkaBrokerListBuffer.toString());
		
			
		String zookeeperConnectString = ambariService.getKafkaZookeeperConnect();
		String[] zookeepers = zookeeperConnectString.split(",");
		if(zookeepers.length > 0) {
			String[] zookeeperHostPort = zookeepers[0].split(":");
			if(zookeeperHostPort.length == 2) {
				saveToRegistry(RegistryKeys.KAFKA_ZOOKEEPER_HOST, zookeeperHostPort[0]);
				saveToRegistry(RegistryKeys.KAFKA_ZOOKEEPER_CLIENT_PORT, zookeeperHostPort[1]);
			} else {
				LOG.error("KafkaBrokerString["+zookeepers[0] + "] is not in the right format");
			}
			
		} else {
			LOG.error("Kafka Zookeeper connect string["+zookeeperConnectString+"] is not in right format");
		}
		
		
		//TODO: Not sure where to get this value. Can't find it in Ambari
		saveToRegistry(RegistryKeys.KAFKA_ZOOKEEPER_ZNODE_PARENT, "");
		
		LOG.info("Finished Populating Service Registry from Ambari");
	}	
	
	/**
	 * Populates Endpoints for HBase either via Ambari or via Slider
	 * @param ambariService
	 * @param params
	 * @throws Exception
	 */
	private void populateRegistryEndpointsForHBase(AmbariUtils ambariService, ServiceRegistryParams params) throws Exception  {
		
		DeploymentMode hbaseDeploymentMode = params.getHbaseDeploymentMode();
		
		if(hbaseDeploymentMode != null) {
			
			String hbaseZookeeperHost = null;
			String phoenixConnectionUrl = null;
			String hBaseZookeeperParentNode = null;
			String hBaseZookepperCientPort = null;
			
			if(DeploymentMode.SLIDER.equals(hbaseDeploymentMode)) {
				
				String hbaseSliderPublisherUrl = params.getHbaseSliderPublisherUrl();
				if(StringUtils.isEmpty(hbaseSliderPublisherUrl)) {
					String errMsg = "hbaseSliderPublisherUrl is required";
					LOG.error(errMsg);
					throw new Exception(errMsg);
				}
				
				HBaseSliderUtils hbaseSliderService = new HBaseSliderUtils(hbaseSliderPublisherUrl);
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating HBase endpoints from HBase Slider Service");
				}
				
				hBaseZookeeperParentNode = hbaseSliderService.getHBaseZookeeperParentNode();
				hBaseZookepperCientPort = hbaseSliderService.getHBaseZookepperCientPort();
				List<String> hbaseZookeeperQuorumList = hbaseSliderService.getHBaseZookeeperQuorum();
				hbaseZookeeperHost = hbaseZookeeperQuorumList.get(0);	
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating HBase endpoints from HBaseSliderUtils");
				}				
				
			} else if(DeploymentMode.STANDALONE.equals(hbaseDeploymentMode) && ambariService != null) {
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating HBase endpoints from Ambari");
				}
				
				hBaseZookeeperParentNode = ambariService.getHBaseZookeeperParentNode();
				hBaseZookepperCientPort = ambariService.getHBaseZookepperCientPort();
				List<String>  hbaseZookeeperQuorumList = ambariService.getHBaseZookeeperQuorum();				
				hbaseZookeeperHost = hbaseZookeeperQuorumList.get(0);	
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating HBase endpoints from Ambari");
				}			
				
			} 
			
			saveToRegistry(RegistryKeys.HBASE_ZOOKEEPER_HOST , hbaseZookeeperHost);	
			saveToRegistry(RegistryKeys.HBASE_ZOOKEEPER_ZNODE_PARENT , hBaseZookeeperParentNode);
			saveToRegistry(RegistryKeys.HBASE_ZOOKEEPER_CLIENT_PORT , hBaseZookepperCientPort);	
			/* From the HBase info, we can create phoenixConnectionUrl */
			phoenixConnectionUrl = "jdbc:phoenix:"+hbaseZookeeperHost+ ":" + hBaseZookepperCientPort + ":" + hBaseZookeeperParentNode;
			saveToRegistry(RegistryKeys.PHOENIX_CONNECTION_URL, phoenixConnectionUrl);
					
		} else {
			LOG.info("Deployment Mode for HBase not configured. Skipping loading HBase Registry values.");
		}
		
	}
	
	/**
	 * Populates the Registry for Storm either via Ambari or Storm
	 * @param ambariService
	 * @param params
	 * @throws Exception
	 */
	private void populateRegistryEndpointsForStorm(AmbariUtils ambariService, ServiceRegistryParams params) throws Exception  {
		
		DeploymentMode stormDeploymentMode = params.getStormDeploymentMode();
		
		if(stormDeploymentMode != null) {
			String stormNimbusHost = null;
			String stormNimbusPort = null;
			String stormUIServer = null;
			String stormUIPort = null;
			String rawStormZookeeperQuorum = null;
			
			if(DeploymentMode.SLIDER.equals(stormDeploymentMode)) {
				
				String stormSliderPublisherUrl = params.getStormSliderPublisherUrl();
				if(StringUtils.isEmpty(stormSliderPublisherUrl)) {
					String errMsg = "stormSliderPublisherUrl is required";
					LOG.error(errMsg);
					throw new Exception(errMsg);
				}
				StormSliderUtils stormSliderService = new StormSliderUtils(stormSliderPublisherUrl);
		
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating Storm endpoints from Storm Slider Service");
				}
				
				stormNimbusHost = stormSliderService.getStormNimbusHost();
				stormNimbusPort = stormSliderService.getStormNimbusPort();
				stormUIServer = stormSliderService.getStormUIServer();
				stormUIPort = stormSliderService.getStormUIPort();
				rawStormZookeeperQuorum = stormSliderService.getStormZookeeperQuorum();
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating Storm endpoints from StormSliderUtils");
				}				
				
			} else if (DeploymentMode.STANDALONE.equals(stormDeploymentMode) && ambariService != null) {
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating Storm endpoints from Ambari");
				}
				
				stormNimbusHost = ambariService.getStormNimbusHost();
				stormNimbusPort = ambariService.getStormNimbusPort();
				stormUIServer = ambariService.getStormUIServer();
				stormUIPort = ambariService.getStormUIPort();
				rawStormZookeeperQuorum =  ambariService.getStormZookeeperQuorum();
							

				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating Storm endpoints from Ambari");
				}			
				
			}
			
			saveToRegistry(RegistryKeys.STORM_NIMBUS_HOST, stormNimbusHost);
			saveToRegistry(RegistryKeys.STORM_NIMBUS_PORT , stormNimbusPort);
			saveToRegistry(RegistryKeys.STORM_UI_HOST, stormUIServer);
			saveToRegistry(RegistryKeys.STORM_UI_PORT, stormUIPort);

			String parsedStormZookeeperQuorum = rawStormZookeeperQuorum.replace("['", "").replace("']","").replace("'", "");
			saveToRegistry(RegistryKeys.STORM_ZOOKEEPER_QUORUM , parsedStormZookeeperQuorum);				
			
		} else {
			LOG.info("Deployment Mode for Storm not configured. Skipping loading Storm Registry values.");
		}
	}		
	
	private void populateRegistryEndpointsForStorm(ServiceRegistryParams params) throws Exception {
		populateRegistryEndpointsForStorm(null, params);
		
	}


	private void populateRegistryEndpointsForHBase(ServiceRegistryParams params) throws Exception {
		populateRegistryEndpointsForHBase(null, params);
		
	}	

	/**
	 * Returns the the entire registry
	 */
	public Map<String, String> getRegistry() {
		return registry;
	}
	
	public String getHBaseDeploymentMode() {
		return getValueForKey(RegistryKeys.HBASE_DEPLOYMENT_MODE);
	}	
	
	
	public String getHBaseZookeeperHost() {
		return getValueForKey(RegistryKeys.HBASE_ZOOKEEPER_HOST);
	}	
	
	public String getHBaseZookeeperClientPort() {
		return getValueForKey(RegistryKeys.HBASE_ZOOKEEPER_CLIENT_PORT);
	}	

	public String getHBaseZookeeperZNodeParent() {
		return getValueForKey(RegistryKeys.HBASE_ZOOKEEPER_ZNODE_PARENT);
	}	

	private String getHBaseSliderPublisherURL() {
		return getValueForKey(RegistryKeys.HBASE_SLIDER_PUBLISHER_URL);
	}
	
	public String getPhoenixConnectionURL() {
		return getValueForKey(RegistryKeys.PHOENIX_CONNECTION_URL);
	}
	
	public String getStormZookeeperQuorum() {
		return getValueForKey(RegistryKeys.STORM_ZOOKEEPER_QUORUM);
	}
	
	public String getStormNimbusPort() {
		return getValueForKey(RegistryKeys.STORM_NIMBUS_PORT);
	}	
	
	public String getStormNimbusHost() {
		return getValueForKey(RegistryKeys.STORM_NIMBUS_HOST);
	}	
	
	private String getStormDeploymentMode() {
		return getValueForKey(RegistryKeys.STORM_DEPLOYMENT_MODE);
	}

	
	public String getStormUIUrl() {
		String stormUIHost = getValueForKey(RegistryKeys.STORM_UI_HOST);
		String stormUIPort = getValueForKey(RegistryKeys.STORM_UI_PORT);
		String url = null;
		if(StringUtils.isNotEmpty(stormUIHost) && StringUtils.isNotEmpty(stormUIPort)) {
			url = "http://"+stormUIHost + ":" + stormUIPort;
		}
		return url;
	}	
	
	private String getStormSliderPublisherURL() {
		return getValueForKey(RegistryKeys.STORM_SLIDER_PUBLISHER_URL);
	}	
	
	public String getKafkaBrokerList() {
		return getValueForKey(RegistryKeys.KAFKA_BROKER_LIST);
	}		
	
	public List<String> getStormZookeeperQuorumAsList() {
		List<String> stormQuorumList = new ArrayList<String>();
		String zooKeeperQuorum =  getValueForKey(RegistryKeys.STORM_ZOOKEEPER_QUORUM);
		if(StringUtils.isNotEmpty(zooKeeperQuorum)) {
			String[] quorumArray = zooKeeperQuorum.split(",");
			stormQuorumList = Arrays.asList(quorumArray);
		}
		return stormQuorumList;
	}		
	
	public String getKafkaZookeeperHost() {
		return getValueForKey(RegistryKeys.KAFKA_ZOOKEEPER_HOST);
	}	
	
	public String getKafkaZookeeperClientPort() {
		return getValueForKey(RegistryKeys.KAFKA_ZOOKEEPER_CLIENT_PORT);
	}		
	
	public String  getKafkaZookeeperZNodeParent() {
		return getValueForKey(RegistryKeys.KAFKA_ZOOKEEPER_ZNODE_PARENT);
	}		
	
	public String getHDFSUrl() {
		return getValueForKey(RegistryKeys.HDFS_URL);
	}	
	
	public String getHiveMetaStoreUrl() {
		return getValueForKey(RegistryKeys.HIVE_METASTORE_URL);
	}	
	
	public String getHiveServer2ConnectionURL() {
		return getValueForKey(RegistryKeys.HIVE_SERVER2_CONNECT_URL);
	}	
	


	public String getFalconServerUrl() {
		return getValueForKey(RegistryKeys.FALCON_SERVER_URL);
	}	
	
	public String getFalconBrokerUrl() {
		return getValueForKey(RegistryKeys.FALCON_BROKER_URL);
	}
	
	public String getFalconServerPort() {
		return getValueForKey(RegistryKeys.FALCON_SERVER_PORT);
	}		
	
	public String getAmbariServerUrl() {
		return getValueForKey(RegistryKeys.AMBARI_SERVER_URL);
	}
	
	public String getOozieUrl() {
		return getValueForKey(RegistryKeys.OOZIE_SERVER_URL);
	}	
	
	public String getClusterName() {
		return getValueForKey(RegistryKeys.AMBARI_CLUSTER_NAME);
	}	

	public String getResourceManagerURL() {
		return getValueForKey(RegistryKeys.RESOURCE_MANGER_URL);
	}
	
	public String getResourceManagerUIURL() {
		String url = null;
		String hostAndPort = getValueForKey(RegistryKeys.RESOURCE_MANGER_URI_URL);
		if(StringUtils.isNotEmpty(hostAndPort)) {
			url = "http://"+hostAndPort;
		}
		return url;
	}	
	



	protected String getValueForKey(String keyName) throws RuntimeException {
		String value = registry.get(keyName);
		if(LOG.isDebugEnabled()) {
			LOG.debug("Value for key["+keyName + "] is:  " + value);
		}
		return value;
	}
	
	private String constructAmbariRestURL(String ambariUrl, String clusterName) {
		
		String ambariRestUrl = null;
		if(StringUtils.isNotEmpty(ambariUrl) && StringUtils.isNotEmpty(clusterName)){
			StringBuffer buffer = new StringBuffer();
			buffer.append(ambariUrl)
				  .append("/api/v1/clusters/")
				  .append(clusterName);
			ambariRestUrl = buffer.toString();			
		}
		return ambariRestUrl;

	}	
	
	public void saveToRegistry(String key, String value) {
		if(key!= null) {
			registry.put(key, value);			
		} else {
			LOG.error("Skipping persisting key["+key + "] into service registry because it is null");
		}	
	}

	private InputStream createConfigInputStream(boolean isAbsolutePath) throws Exception {
		if(isAbsolutePath) {
			return createConfigInputStreamFromAbsolute(); 
		} else {
			return createConfigInputStreamFromRelative();
		}
	}	
	
	private InputStream createConfigInputStreamFromAbsolute() throws Exception {
		InputStream inputStream = null;
		String fileName = null;
		try {
			fileName = constructFullFileName(this.hdpServiceRegistryConfigLocation, this.configFileName);
			inputStream = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			String errMsg = "Could not load service config["+fileName + "]";
			LOG.error(errMsg);
			throw new RuntimeException(errMsg);
		}
		
		return inputStream;
	}
	
	private InputStream createConfigInputStreamFromRelative() throws Exception {
		String fileName = null;
		fileName = constructFullFileName(this.hdpServiceRegistryConfigLocation, this.configFileName);
		InputStream inputStream = this.getClass().getResourceAsStream(fileName);
		if(inputStream == null) {
			String errMsg = "Could not load service config["+fileName + "]";
			LOG.error(errMsg);	
			throw new RuntimeException(errMsg);			
		}
		
		return inputStream;
	}	

	private String constructFullFileName(String location, String fileName) {
		return location + "/" + fileName;
	}

	@Override
	public String getCustomValue(String key) {
		String value = null;
		if(StringUtils.isNotEmpty(key)) {
			value = getValueForKey(key);
		}
		return value;
	}


	
}
