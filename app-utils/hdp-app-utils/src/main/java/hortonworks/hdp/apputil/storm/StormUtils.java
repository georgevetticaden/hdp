package hortonworks.hdp.apputil.storm;


import hortonworks.hdp.apputil.registry.HDPServiceRegistry;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;


public class StormUtils {

	
	private static final Logger LOG = Logger.getLogger(StormUtils.class);

	private HDPServiceRegistry serviceRegistry;

	
	
	public StormUtils(HDPServiceRegistry serviceRegistry) {
		this.serviceRegistry = serviceRegistry;

	}
	
	/*
	 * Uploads and deploys a Storm Topology Jar
	 */
	public void deployStormTopology(StormTopologyParams topologyParams) throws Exception {

		/* Create the storm client config */
		Map stormClientConfiguration = constructStormClientConfig();
		
		/* Upload topology jar to Cluster */
		String uploadedJarLocation;
		if(topologyParams.isUpload()) {
			uploadedJarLocation = upload(stormClientConfiguration, topologyParams.getTopologyJarLocation());
		} else {
			LOG.info("Skipping upload. Assumeing jar already uploaded here: " + topologyParams.getUploadedTopologyJarLocation());
			uploadedJarLocation = topologyParams.getUploadedTopologyJarLocation();
		}

		/* Upload the storm topology */
		deploy(topologyParams.getTopologyName(), topologyParams.getTopology(), stormClientConfiguration, uploadedJarLocation);
		
		Thread.sleep(20000);
	}
	
	
	/*
	 * Kills the Storm Topology if its up
	 */
	public void killStormTopology(String topologyName) throws Exception {
	
		
		/* Create the storm client config */
		Map stormClientConfiguration = constructStormClientConfig();	
		NimbusClient nimbus = createNimbusClient(stormClientConfiguration);
		TopologySummary topologySummary = getStormTopologySummary(topologyName);
		
		if(topologySummary != null) {
			LOG.info("Topology["+topologyName+ "] is up. Killing it first before deploying");
			KillOptions killOpts = new KillOptions();
			killOpts.set_wait_secs(1);
			nimbus.getClient().killTopologyWithOpts(topologyName, killOpts);
			LOG.info("Topology["+topologyName+ "]  killed successfully");
		} else {
			LOG.info("Topology["+topologyName + "] is not up. There is nothing to kill");
		}
	}
	
	
	public TopologySummary getStormTopologySummary(String topologyName) throws Exception {
		
		
		Map stormClientConfiguration = constructStormClientConfig();	
		NimbusClient nimbus = createNimbusClient(stormClientConfiguration);
		List<TopologySummary> summaries =  nimbus.getClient().getClusterInfo().get_topologies();
		TopologySummary summary = null;
		for(TopologySummary topSummary: summaries) {
			if(topSummary.get_name().equals(topologyName)) {
				summary = topSummary;
				break;
			}
		}
		return summary;
	}

	private void deploy(String topologyName, StormTopology topology,
			Map stormClientConfiguration, String uploadedJarLocation)
			throws Exception{
		
		NimbusClient nimbus = createNimbusClient(stormClientConfiguration);
		try {
			String jsonConf = JSONValue.toJSONString(stormClientConfiguration);
			
			//kill first if its up
			killStormTopology(topologyName);
			
			//wait for 10 seconds
			Thread.sleep(10000);
			
			//deploy
			LOG.info("Started Deployment of topology["+ topologyName+"]");
			nimbus.getClient().submitTopology(topologyName,
					uploadedJarLocation, jsonConf, topology);
			LOG.info("Completed Deployment of topology["+ topologyName+"]");
			
		} catch (Exception ae) {
			String errMsg = "Error Deploying Storm topology";
			LOG.error(errMsg, ae);
			throw new RuntimeException(errMsg, ae);
		}
	}

	private NimbusClient createNimbusClient(Map stormClientConfiguration)
			throws Exception {
		NimbusClient nimbus = new NimbusClient(stormClientConfiguration,serviceRegistry.getStormNimbusHost(), Integer.valueOf(serviceRegistry.getStormNimbusPort()));
		return nimbus;
	}

	public String upload(Map stormClientConfiguration, String topologyJarLocation) {
		
		if(StringUtils.isEmpty(topologyJarLocation)) {
			String errMsg = "Property[storm.topology.jar] must be configured in storm-topology-config.properies file";
			throw new RuntimeException(errMsg);
		}
		
		LOG.info("Uploading storm topology Jar["+topologyJarLocation+"] to Nimbus Host["+stormClientConfiguration.get("nimbus.host") + "]");
		String uploadedJarLocation = StormSubmitter.submitJar(stormClientConfiguration,  topologyJarLocation);
		LOG.info("Finished uploading storm topology Jar["+topologyJarLocation+"] to Nimbus Host["+stormClientConfiguration.get("nimbus.host") + "] in the following location["+uploadedJarLocation +"]");
		
		return uploadedJarLocation;
	}



	private Map constructStormClientConfig() {
		Map stormClientConfiguration = Utils.readStormConfig();
		stormClientConfiguration.put("nimbus.host", serviceRegistry.getStormNimbusHost());
		stormClientConfiguration.put("storm.zookeeper.servers", serviceRegistry.getStormZookeeperQuorumAsList());
		int nimbusPort = Integer.valueOf(serviceRegistry.getStormNimbusPort());
		stormClientConfiguration.put("nimbus.thrift.port", nimbusPort);
		
		//set numberOfWorkers
		//TODO: Not sure if we need to populate the number of topology workers. For not commenting out
		//Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("storm.trucker.topology.workers"));
		//stormClientConfiguration.put(Config.TOPOLOGY_WORKERS, topologyWorkers);		
		return stormClientConfiguration;
	}
	

}
