package hortonworks.hdp.apputil.slider.storm;

import java.io.IOException;
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

public class StormSliderUtils {

	private static final Logger LOG = Logger.getLogger(StormSliderUtils.class);
	private RestTemplate restTemplate;
	private ObjectMapper mapper;
	private String stormSliderPublisherUrl;
	
	
	public StormSliderUtils(String stormSliderPublisherUrl) {
		this.stormSliderPublisherUrl = stormSliderPublisherUrl;
		restTemplate = new RestTemplate();
		this.mapper = new ObjectMapper();
	}	

	
	
	public String getStormZookeeperQuorum() throws Exception {
		return getStormConfigurationValueFor("storm.zookeeper.servers");
	}	
	

	public String getStormNimbusPort() throws Exception {
		return getStormConfigurationValueFor("nimbus.thrift.port");
	}
	
	public String getStormNimbusHost() throws Exception {
		return getStormConfigurationValueFor("nimbus.host");
	}	
	
	public String getStormUIServer() throws Exception {
		String stormUIServerPort = getStormQuickLinkFor("org.apache.slider.monitor");
		String args[] = stormUIServerPort.split(":");
		String uiServer = null;
		if(args.length == 3) {
			String server = args[1];
			if(server.startsWith("//")) {
				uiServer = server.substring(2);
			}
			
		}
		return uiServer;
	}	
	
	public String getStormUIPort() throws Exception {
		return getStormConfigurationValueFor("ui.port");
	}
	
	
	
	private String getStormConfigurationValueFor(String property) throws Exception{
		String restUrl = stormSliderPublisherUrl + "/slider/storm-site";
		Map<String, Object> result = executeRESTGetCall(restUrl);
		Map<String, Object> hbasePropertiesMap = (Map<String, Object>) result.get("entries");
		return (String)hbasePropertiesMap.get(property);
	}
	
	private String getStormQuickLinkFor(String property) throws Exception{
		String restUrl = stormSliderPublisherUrl + "/slider/quicklinks";
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
			LOG.info("Executing StormSliderUtils Rest Call["+url + "]");
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
