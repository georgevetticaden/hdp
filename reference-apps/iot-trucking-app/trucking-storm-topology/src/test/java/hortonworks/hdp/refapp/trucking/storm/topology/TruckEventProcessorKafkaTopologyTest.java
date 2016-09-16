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
	public void deployPhase1Topology() throws Exception{
		
		StormTopologyParams topologyParams = createPhase1TopologyParams();
		stormUtils.deployStormTopology(topologyParams);
	}
	
	@Test
	public void deployPhase2Topology() throws Exception{
		
		StormTopologyParams topologyParams = createPhase2TopologyParams();
		stormUtils.deployStormTopology(topologyParams);
	}	
	
	@Test
	public void deployPhase3Topology() throws Exception{
		
		StormTopologyParams topologyParams = createPhase3TopologyParams();
		stormUtils.deployStormTopology(topologyParams);
	}		


	private StormTopologyParams createPhase1TopologyParams() throws Exception {

		StormTopology topology = createPhase1Topology(topologyConfig);
		
		StormTopologyParams topologyParams = new StormTopologyParams();
		topologyParams.setUpload(true);	
		topologyParams.setTopology(topology);
		topologyParams.setTopologyName(topologyConfig.getProperty("trucking.topology.name"));
		topologyParams.setTopologyJarLocation(topologyConfig.getProperty(STORM_TOPOLOGY_KEY));
		topologyParams.setNumberOfWorkers(Integer.valueOf(topologyConfig.getProperty("trucking.storm.trucker.topology.workers")));
		topologyParams.setTopologyEventLogExecutors(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.eventlogger.executors")));
		topologyParams.setTopologyMessageTimeoutSecs(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.message.timeout.secs")));
		
		
		Properties prop = new Properties();
		prop.put("storm.zookeeper.connection.timeout", 30000);
		topologyParams.setCustomStormProperties(prop);
		
		
		return topologyParams;
	}
	
	private StormTopologyParams createPhase2TopologyParams() throws Exception {

		StormTopology topology = createPhase2Topology(topologyConfig);
		
		StormTopologyParams topologyParams = new StormTopologyParams();
		topologyParams.setUpload(true);	
		topologyParams.setTopology(topology);
		topologyParams.setTopologyName("streaming-analytics-ref-app-phase2");
		topologyParams.setTopologyJarLocation(topologyConfig.getProperty(STORM_TOPOLOGY_KEY));
		topologyParams.setNumberOfWorkers(Integer.valueOf(topologyConfig.getProperty("trucking.storm.trucker.topology.workers")));
		topologyParams.setTopologyEventLogExecutors(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.eventlogger.executors")));
		topologyParams.setTopologyMessageTimeoutSecs(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.message.timeout.secs")));
		
		Properties prop = new Properties();
		prop.put("storm.zookeeper.connection.timeout", 30000);
		topologyParams.setCustomStormProperties(prop);		
		
		return topologyParams;
	}
	
	private StormTopologyParams createPhase3TopologyParams() throws Exception {

		StormTopology topology = createPhase3Topology(topologyConfig);
		
		StormTopologyParams topologyParams = new StormTopologyParams();
		topologyParams.setUpload(true);	
		topologyParams.setTopology(topology);
		topologyParams.setTopologyName("streaming-analytics-ref-app-phase3");
		topologyParams.setTopologyJarLocation(topologyConfig.getProperty(STORM_TOPOLOGY_KEY));
		topologyParams.setNumberOfWorkers(Integer.valueOf(topologyConfig.getProperty("trucking.storm.trucker.topology.workers")));
		topologyParams.setTopologyEventLogExecutors(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.eventlogger.executors")));
		topologyParams.setTopologyMessageTimeoutSecs(Integer.valueOf(topologyConfig.getProperty("trucking.storm.topology.message.timeout.secs")));
		
		Properties prop = new Properties();
		prop.put("storm.zookeeper.connection.timeout", 30000);
		topologyParams.setCustomStormProperties(prop);		
		
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

	private StormTopology createPhase1Topology(Properties topologyConfig) throws Exception {
		
		/* Construct the Topology */
		TruckEventProcessorKafkaTopology truckTopology = new TruckEventProcessorKafkaTopology(topologyConfig);
		StormTopology topology = truckTopology.buildTopology();
		return topology;
	}
	
	private StormTopology createPhase2Topology(Properties topologyConfig) throws Exception {
		
		/* Construct the Topology */
		TruckEventProcessorKafkaTopologyPhase2 truckTopology = new TruckEventProcessorKafkaTopologyPhase2(topologyConfig);
		StormTopology topology = truckTopology.buildTopology();
		return topology;
	}	
	private StormTopology createPhase3Topology(Properties topologyConfig) throws Exception {
		
		/* Construct the Topology */
		TruckEventProcessorKafkaTopologyPhase3 truckTopology = new TruckEventProcessorKafkaTopologyPhase3(topologyConfig);
		StormTopology topology = truckTopology.buildTopology();
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
	
	


}
