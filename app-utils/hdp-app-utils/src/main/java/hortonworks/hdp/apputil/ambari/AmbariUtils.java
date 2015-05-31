package hortonworks.hdp.apputil.ambari;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AmbariUtils {

	private static final Logger LOG = Logger.getLogger(AmbariUtils.class);
	private RestTemplate restTemplate;
	private ObjectMapper mapper;
	private String ambariRESTUrl;
	
	
	public AmbariUtils(String ambariRESTURL) {
		
		if(StringUtils.isEmpty(ambariRESTURL)) {
			throw new RuntimeException("RestuURL cannot not be empty");
		}
		this.ambariRESTUrl = ambariRESTURL;
		restTemplate = new RestTemplate();
		this.mapper = new ObjectMapper();
	}

	/* Zookeeper Details */
	
	public List<String> getHBaseZookeeperQuorum() throws Exception {
		String quorumString =  getLatestConfigurationValueFor("hbase-site", "hbase.zookeeper.quorum");
		String[] zookeepers = quorumString.split(",");
		List<String> quorumList = new ArrayList<String>();
		
		if(zookeepers != null) {
			for(String zookeeper: zookeepers) {
				quorumList.add(zookeeper);
			}
		}
		return quorumList;
		
	}


	/* HBase Information */
	public String getHBaseZookepperCientPort() throws Exception{
		return  getLatestConfigurationValueFor("hbase-site", "hbase.zookeeper.property.clientPort");
	}
	
	public String getHBaseZookeeperParentNode() throws Exception {
		return  getLatestConfigurationValueFor("hbase-site", "zookeeper.znode.parent");
	}	
	
	/* Storm Information */
	public String getStormZookeeperQuorum() throws Exception {
		return getLatestConfigurationValueFor("storm-site", "storm.zookeeper.servers");
	}	
	
	public String getStormNimbusPort() throws Exception {
		return getLatestConfigurationValueFor("storm-site", "nimbus.thrift.port");
	}
	
	public String getStormNimbusHost() throws Exception {
		return getLatestConfigurationValueFor("storm-site", "nimbus.host");
	}	
	
	public String getStormUIServer() throws Exception {
		String stormUIServer = null;
		List<String> hosts = getHostNameForServiceAndComponent("STORM", "STORM_UI_SERVER");
		if(!hosts.isEmpty()) {
			stormUIServer = hosts.get(0);
		}
		return stormUIServer ;
	}	
	
	public String getStormUIPort() throws Exception {
		return getLatestConfigurationValueFor("storm-site", "ui.port");
	}		
	
	/* HDFS information */
	public String getHDFSUrl() throws Exception  {
		return getLatestConfigurationValueFor("core-site", "fs.defaultFS");
	}	
	
	/* Hive Information */
	public String getHiveMetaStoreUrl() throws Exception  {
		return getLatestConfigurationValueFor("hive-site", "hive.metastore.uris");
	}
	
	public String getHiveServer2Host() throws Exception {
		List<String> hosts =  getHostNameForServiceAndComponent("HIVE", "HIVE_SERVER");
		String hiveServer2Host = null;
		if(!hosts.isEmpty()) {
			hiveServer2Host = hosts.get(0);
		}
		return hiveServer2Host ;			
	}
	
	public String getHiveServer2ThriftPort() throws Exception {
		return getLatestConfigurationValueFor("hive-site", "hive.server2.thrift.port");
	}
	
	/* Falcon Infomration */
	public String getFalconHost() throws Exception {
		String falconHost = null;
		List<String> hosts = getHostNameForServiceAndComponent("FALCON", "FALCON_SERVER");
		if(!hosts.isEmpty()) {
			falconHost = hosts.get(0);
		}
		return falconHost ;		
	}
	
	public String getFalconBrokerUrl() throws Exception {
		return getLatestConfigurationValueFor("falcon-startup.properties", "*.broker.url");
	}
	
	
	/* Yarn Information */
	public String getResourceManagerUrl() throws Exception {
		return getLatestConfigurationValueFor("yarn-site", "yarn.resourcemanager.address");
	}
	
	public String getResourceManagerUIUrl() throws Exception {
		return getLatestConfigurationValueFor("yarn-site", "yarn.resourcemanager.webapp.address");
	}	
	
	
	/* Oozie Information */
	public String getOozieServerUrl() throws Exception {
		return getLatestConfigurationValueFor("oozie-site", "oozie.base.url");
	}
	
	/* Kafka Information */

	public List<String> getKafkaBrokerList() throws Exception  {
		return getHostNameForServiceAndComponent("KAFKA", "KAFKA_BROKER");

	}
	
	public String getKafkaBrokerPort() throws Exception {
		return getLatestConfigurationValueFor("kafka-broker", "port");
	}

	public String getKafkaZookeeperConnect() throws Exception {
		return getLatestConfigurationValueFor("kafka-broker", "zookeeper.connect");
	}

	

	private List<String> getHostNameForServiceAndComponent(String service, String component) throws IOException,
			JsonParseException, JsonMappingException {
		Map<String, String> mapParams = new HashMap<>();
		mapParams.put("service", service);
		mapParams.put("component", component);
		String url = constructRESTUrl("/services/{service}/components/{component}");
		Map<String, Object> zookeerInfo = executeRESTGetCall(url, mapParams);
	
		List<Map<String, Object>> hostComponents = (List<Map<String, Object>>) zookeerInfo.get("host_components");
		
		List<String> hosts = new ArrayList<String>();
		for(Map<String, Object> hostComponent: hostComponents) {
			String hostName = ((Map<String, String>)hostComponent.get("HostRoles")).get("host_name");
			hosts.add(hostName);
		}
		return hosts;
	}	
	
	private String getLatestConfigurationValueFor(String configurationType, String property) throws Exception {
		String actionUrl = "/configurations?type="+configurationType;
		String restUrl = constructRESTUrl(actionUrl);
		List<Map<String, Object>> hbaseConfig = (List<Map<String, Object>>)executeRESTGetCall(restUrl).get("items");
		
		restUrl = (String) hbaseConfig.get(hbaseConfig.size()-1).get("href");
		
		

		Map<String, Object> hbasePropertiesMap = executeRESTGetCall(restUrl);
		List<Map<String, Object>> hbasePropertiesList = (List<Map<String, Object>>) hbasePropertiesMap.get("items");
		Map<String, String> hbaseProperties =  (Map<String, String>) hbasePropertiesList.get(0).get("properties");
		return hbaseProperties.get(property);
	}
	
	
	
	
	protected Map<String, Object> executeRESTGetCall(String url)
			throws IOException, JsonParseException, JsonMappingException {
		return executeRESTGetCall(url, null);
	}


	private Map<String, Object> executeRESTGetCall(String url, Map<String, String> params)
			throws IOException, JsonParseException, JsonMappingException {		
		
		try {
			String responseBody = getResponseBody(url, params);
			return mapper.readValue(responseBody, Map.class);			
		} catch (Exception e) {
			String errMsg = "Error executing Rest Call["+ url + "] with params["+params+"]";
			LOG.error(errMsg, e);
			throw e;
		}

	}


	

	private HttpHeaders constructAuthHeader() {
		String plainCreds = "admin:admin";
		byte[] plainCredsBytes = plainCreds.getBytes();
		byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
		String base64Creds = new String(base64CredsBytes);	
		
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Basic " + base64Creds);
		
		return headers;
	}

	public String collectClusterBluePrint(String fileToStoreBluePrint) {
		String url = constructRESTUrl("?format=blueprint");
		return getResponseBody(url, null);
	}

	
	private String getResponseBody(String url, Map<String, String> params) {
		ResponseEntity<String> response = null;
		if(params != null)
			response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<String>(constructAuthHeader()), String.class, params);
		else
			response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<String>(constructAuthHeader()), String.class);	
		String responseBody = response.getBody();
		return responseBody;
	}

	private String constructRESTUrl(String actionUrl) {
		return ambariRESTUrl + actionUrl;
	}	

}
