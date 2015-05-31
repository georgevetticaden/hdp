package hortonworks.hdp.apputil.falcon;


import hortonworks.hdp.apputil.registry.HDPServiceRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.VelocityException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.ui.velocity.VelocityEngineFactoryBean;
import org.springframework.ui.velocity.VelocityEngineUtils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;



public class FalconUtils {
	
	private static final Logger LOG = Logger.getLogger(FalconUtils.class);
	private static final String USER_SUBMITTING_JOB = "user.name=hdfs";
	
	private RestTemplate restTemplate;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	private VelocityEngine velocityEngine;
	
	private HDPServiceRegistry serviceRegistry;
	
	
	public FalconUtils(HDPServiceRegistry serviceRegistry) {
		this.restTemplate = new RestTemplate();
		this.serviceRegistry = serviceRegistry;
		this.velocityEngine = velocityEngine();
	}
	
	
	public void submitClusterEntity(String file) throws Exception{
		String request = velocitizeClusterConfig(file);
		submitEntity("cluster", "submit",  request);
	}
	
	public void submitAndScheduleFeedEntity(String file) throws Exception{
		String request = loadEntityFile(file);
		submitEntity("feed", "submitAndSchedule", request);
	}	
	
	public void submitAndScheduleProcessEntity(String file) throws Exception{
		String request = loadEntityFile(file);
		submitEntity("process", "submitAndSchedule",  request);
	}	
	
	
	public void deleteClusterEntity(String clusterName) throws Exception{
		deleteEntity("cluster", clusterName);
	}
	
	public void deleteFeedEntity(String feedName) throws Exception{
		deleteEntity("feed", feedName);
	}	

	public void deleteProcessEntity(String processName) throws Exception{
		deleteEntity("process", processName);
	}		

	private void deleteEntity(String entityType, String entityName) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("action", "delete");
		params.put("entityType", entityType);
		params.put("entityName", entityName);
		String url = serviceRegistry.getFalconServerUrl() + "/api/entities/{action}/{entityType}/{entityName}?"+USER_SUBMITTING_JOB;
		
		try {
			LOG.info("Entity Type["+entityType + "], entityName["+entityName +"] being deleted");
			restTemplate.delete(url, params);
		} catch (RestClientException e) {
			String errMsg = "Error deleting Entity Type["+entityType + "], entityName["+entityName +"]";
			LOG.error(errMsg);
			throw new RuntimeException(errMsg, e);
		}
	}		

	private void submitEntity (String entityType, String action,  String request) throws Exception {
		
		Map<String, String> params = new HashMap<String, String>();
		params.put("action", action);
		params.put("entityType", entityType);
		String url = serviceRegistry.getFalconServerUrl() + "/api/entities/{action}/{entityType}?"+USER_SUBMITTING_JOB;
		try {
			LOG.debug("Entity Type["+entityType + "], action["+ action + "], submitting request....." + request);		
			restTemplate.postForLocation(url, request, params);
		} catch (RestClientException e) {
			String errorMsg = "Error Submitting Entity Type["+entityType + "], action["+ action + "], submitting request....." + request;
			LOG.error(errorMsg);
			throw new RuntimeException(errorMsg, e);
		}
	}
	

	
	public String loadEntityFile(String fileName) throws Exception {
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream(fileName);
		return IOUtils.toString(inputStream);
		
	}


	public String velocitizeClusterConfig(String fileName) throws Exception {
		Map<String, Object> model = new HashMap<String, Object>();
		model.put("hftp_url", serviceRegistry.getHDFSUrl());
		model.put("hdfs_url", serviceRegistry.getHDFSUrl());
		model.put("resourcemanager_url",serviceRegistry.getResourceManagerURL());
		model.put("oozie_url", serviceRegistry.getOozieUrl()); 
		model.put("hive_metastore_url", serviceRegistry.getHiveMetaStoreUrl());
		model.put("falcon_broker_url", serviceRegistry.getFalconBrokerUrl());
		
		String text = VelocityEngineUtils.mergeTemplateIntoString(velocityEngine, fileName, model);
		return text;
	}
	
 	public VelocityEngine velocityEngine() {
 		try {
 	 	    VelocityEngineFactoryBean factory = new VelocityEngineFactoryBean();
 	 	    Properties props = new Properties();
 	 	    props.put("resource.loader", "class");
 	 	    props.put("class.resource.loader.class",
 	 	              "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
 	 	    factory.setVelocityProperties(props);
 	 	    
 	 	    return factory.createVelocityEngine(); 			
 		} catch (Exception e) {
 			String errorMsg = "Cannot initialize velocity Engine";
 			LOG.error(errorMsg, e);
 			throw new RuntimeException(errorMsg, e);
 		}

 	}	

	

}

