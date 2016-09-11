package hortonworks.hdp.refapp.trucking.install.web;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.storm.StormTopologyParams;
import hortonworks.hdp.refapp.trucking.config.registry.HDPRefAppServiceRegistryConfig;
import hortonworks.hdp.refapp.trucking.install.service.StormService;
import hortonworks.hdp.refapp.trucking.monitor.CustomHDPServiceRegistryParams;
import hortonworks.hdp.refapp.trucking.registry.CustomRegistryKeys;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


@Controller
public class TruckAppInstallerController {

	private static final Logger LOG = LoggerFactory.getLogger(TruckAppInstallerController.class);
	
	public static final double STL_LAT= 38.523884;
	public static final double STL_LONG= -92.159845;
	public static final int DEFAULT_ZOOME_LEVEL = 6;
	public static final int DEFAULT_TRUCK_SYMBOL_SIZE = 10000;	

	
	@Autowired
	private HDPServiceRegistry registry;
	
	@Autowired
	private StormService stormService;
	
	

	
	@RequestMapping(value="/iotdemo/truck/install/configureEndpoints", method=RequestMethod.POST)
	public  String configureEndpoints(@RequestBody CustomHDPServiceRegistryParams registryParams, HttpSession session) throws Exception {
		
		LOG.info("Populating Service Registry");
		LOG.info("Ambari Server URL[" + registryParams.getAmbariUrl() + ", cluster name["+registryParams.getClusterName()+" ]");
		LOG.info("HBase Deployment Mode[" + registryParams.getHbaseDeploymentMode() + "], HBase Publisher Url[" + registryParams.getHbaseSliderPublisherUrl());
		LOG.info("Storm Deployment Mode[" + registryParams.getStormDeploymentMode() + "], Storm Publisher Url[" + registryParams.getStormSliderPublisherUrl());
		LOG.info("ActiveMQ Host[" + registryParams.getActiveMQHost() + "]");
		LOG.info("Solr Server URL[" + registryParams.getSolrServerUrl() + "]");
		
		try {
			
			HDPServiceRegistry registry = getServiceRegistry(session);
			Map<String, String> customParamsMap = constructCustomParamsMap(registryParams);
			registry.populate(registryParams, customParamsMap, HDPRefAppServiceRegistryConfig.CONFIG_FILE_NAME);
			
			
		} catch (Exception e) {
			LOG.error("Error populating registry:"+e.getMessage(), e);
			return "ERROR";
		}
		return "SUCCESS"; 
	}	

	
	@RequestMapping(value="/iotdemo/truck/install/deployStormTopology", method=RequestMethod.POST)
	public String configureAndDeployTruckingTopology() {
		LOG.info("inside configure and deploy trucking topology");
		try {
			StormTopologyParams topologyParams = new StormTopologyParams();
			topologyParams.setUpload(true);	
			this.stormService.deployStormTopology(topologyParams);
		} catch (Exception e) {
			LOG.error("Error Deploying Storm Topology", e);
			return "ERROR";
		}
		return "SUCCESS";
	}	
	

	
	private Map<String, String> constructCustomParamsMap(
			CustomHDPServiceRegistryParams registryParams) {
		
		Map<String, String> customParamsMap = new HashMap<String, String>();
		customParamsMap.put(CustomRegistryKeys.ACTIVEMQ_HOST, registryParams.getActiveMQHost());
		customParamsMap.put(CustomRegistryKeys.SOLR_SERVER_URL, registryParams.getSolrServerUrl());
		
		return customParamsMap;
		
	}	
	
	private HDPServiceRegistry getServiceRegistry(HttpSession session) {
		return registry;
	}		
	

}
