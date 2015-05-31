package hortonworks.hdp.apputil.slider.hbase;

import java.io.IOException;
import java.util.ArrayList;
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

public class HBaseSliderUtils {

	private static final Logger LOG = Logger.getLogger(HBaseSliderUtils.class);
	private RestTemplate restTemplate;
	private ObjectMapper mapper;
	private String hbaseSliderPublisherUrl;
	
	
	public HBaseSliderUtils(String hbaseSliderPublisherUrl) {
		this.hbaseSliderPublisherUrl = hbaseSliderPublisherUrl;
		restTemplate = new RestTemplate();
		this.mapper = new ObjectMapper();
	}	
	
	public String getHBaseZookepperCientPort() throws Exception{
		
		return  getHBaseConfigurationValueFor("hbase.zookeeper.property.clientPort");
	}
	
	public String getHBaseZookeeperParentNode() throws Exception {
		return  getHBaseConfigurationValueFor("zookeeper.znode.parent");
	}	
	
	public List<String> getHBaseZookeeperQuorum() throws Exception {
		String quorumString =  getHBaseConfigurationValueFor("hbase.zookeeper.quorum");
		String[] zookeepers = quorumString.split(",");
		List<String> quorumList = new ArrayList<String>();
		
		if(zookeepers != null) {
			for(String zookeeper: zookeepers) {
				quorumList.add(zookeeper);
			}
		}
		return quorumList;
		
	}	
	
	
	
	private String getHBaseConfigurationValueFor(String property) throws Exception {
		String restUrl = hbaseSliderPublisherUrl + "/slider/hbase-site";
		Map<String, Object> result = executeRESTGetCall(restUrl);
		Map<String, Object> hbasePropertiesMap = (Map<String, Object>) result.get("entries");
		return (String)hbasePropertiesMap.get(property);
	}
	
	
	

	protected Map<String, Object> executeRESTGetCall(String url)
			throws IOException, JsonParseException, JsonMappingException {
		return executeRESTGetCall(url, null);
	}


	private Map<String, Object> executeRESTGetCall(String url, Map<String, String> params)
			throws IOException, JsonParseException, JsonMappingException {		
		
		ResponseEntity<String> response = null;
		if(LOG.isInfoEnabled()) {
			LOG.info("Executing HBaseSliderUtils Rest Call["+url + "]");
		}
		if(params != null)
			response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<String>(constructAuthHeader()), String.class, params);
		else
			response = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<String>(constructAuthHeader()), String.class);	
		String responseBody = response.getBody();
		return mapper.readValue(responseBody, Map.class);
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




}
