package hortonworks.hdp.refapp.trucking.storm.topology;

import static org.junit.Assert.assertNotNull;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.storm.StormTopologyParams;
import hortonworks.hdp.apputil.storm.StormUtils;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologySummary;

public class TruckEventProcessorKafkaTopologyTest extends BaseTopologyTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(TruckEventProcessorKafkaTopologyTest.class);
	private static final String STORM_TOPOLOGY_KEY = "trucking.storm.topology.jar";
	

	private static StormUtils stormUtils;
	private static HDPServiceRegistry serviceRegistry;
	private static Properties topologyConfig;
	
	
	@BeforeClass
	public static void setup() throws Exception {

		LOG.debug("Setting up Endpoints with HDP Service Registry");
		serviceRegistry = createHDPServiceRegistry();
		stormUtils = new StormUtils(serviceRegistry);
		topologyConfig = constructStormTopologyConfig(serviceRegistry);
	}
	

	
	@Test
	public void testDeployTopology() throws Exception{
		
		StormTopologyParams topologyParams = createTopologyParams();
		stormUtils.deployStormTopology(topologyParams);
	}


	private StormTopologyParams createTopologyParams() throws Exception {

		StormTopology topology = createTopology(topologyConfig);
		
		StormTopologyParams topologyParams = new StormTopologyParams();
		topologyParams.setUpload(true);	
		topologyParams.setTopology(topology);
		topologyParams.setTopologyName(topologyConfig.getProperty("trucking.topology.name"));
		topologyParams.setTopologyJarLocation(topologyConfig.getProperty(STORM_TOPOLOGY_KEY));
		topologyParams.setNumberOfWorkers(Integer.valueOf(topologyConfig.getProperty("trucking.storm.trucker.topology.workers")));
		topologyParams.setTopologyEventLogExecutors(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.eventlogger.executors")));
		topologyParams.setTopologyMessageTimeoutSecs(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.message.timeout.secs")));
		
		
		return topologyParams;
	}
	
	@Test
	public void testGetStormTopology() throws Exception {

		TopologySummary summary =  stormUtils.getStormTopologySummary(topologyConfig.getProperty("trucking.topology.name"));
		assertNotNull(summary);
		System.out.println(summary.get_id());
	}	
	
	@Test
	public void testKillStormTopology() throws Exception {
		stormUtils.killStormTopology(topologyConfig.getProperty("trucking.topology.name"));
	}	

	private StormTopology createTopology(Properties topologyConfig) throws Exception {
		
		/* Construct the Topology */
		StormTopology topology = buildTopology(topologyConfig);
		return topology;
	}
		
	private static Properties constructStormTopologyConfig(HDPServiceRegistry registry)   {
		Properties topologyConfig = new Properties();
		
		// Dump everything from registry into the Topology Config
		for(String key: registry.getRegistry().keySet()) {
			topologyConfig.put(key, registry.getRegistry().get(key));
		}
		
		//Only extra we need to store is the kafka zookeeper connection string		
		String zookeeperHostPort = serviceRegistry.getKafkaZookeeperHost() + ":" + serviceRegistry.getKafkaZookeeperClientPort();
		topologyConfig.put("kafka.zookeeper.host.port", zookeeperHostPort);
	
		return topologyConfig;
	}	
	
	private StormTopology buildTopology(Properties topologyConfig) throws Exception {
		TruckEventProcessorKafkaTopology truckTopology = new TruckEventProcessorKafkaTopology(topologyConfig);
		StormTopology topology = truckTopology.buildTopology();
		return topology;
	}	
	


}
