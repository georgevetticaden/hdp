package hortonworks.hdp.apputil.falcon;

import static org.junit.Assert.assertNotNull;
import hortonworks.hdp.apputil.BaseUtilsTest;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;
import hortonworks.hdp.apputil.registry.ServiceRegistryParams;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FalconUtilsTest extends BaseUtilsTest {

	private FalconUtils falconUtils;
	
	@Before
	public void initialize() throws Exception {
		HDPServiceRegistry serviceRegistry = createHDPServiceRegistryWithAmbariAndSliderParams(DEFAULT_CONFIG_FILE_NAME, false);
		this.falconUtils = new FalconUtils(serviceRegistry);
	}


	
	@Test
	public void testSubmittingEntirePipeline() throws Exception{
		// First submit the cluster entitity
		String cluster = "/falcon/entities/cluster/primary-cluster.xml";
		falconUtils.submitClusterEntity(cluster);
		
		//then submit two feeds
		String feed = "falcon/entities/feed/raw-data-feed.xml";
		falconUtils.submitAndScheduleFeedEntity(feed);
		String orcDataFeed = "falcon/entities/feed/orc-data-feed.xml";
		falconUtils.submitAndScheduleFeedEntity(orcDataFeed);
		
		/* Test Submit and Schedule Processes */
		String orcConversionProcessFile = "falcon/entities/process/orc-conversion-process.xml";
		falconUtils.submitAndScheduleProcessEntity(orcConversionProcessFile);
	}	
	
	@Test
	public void testTearingDownPipeline() throws Exception{
		
		/* Delete Feeds */	
		String orcIndexProcess = "orc-conversion-process";
		falconUtils.deleteProcessEntity(orcIndexProcess);
		
		/* Delete Feeds */
		String rawDataFeedName = "truck-events-raw-data-feed";
		falconUtils.deleteFeedEntity(rawDataFeedName);
		
		String orcDataFeedName = "truck-events-orc-data-feed";
		falconUtils.deleteFeedEntity(orcDataFeedName);
		
		/* Delete Cluster */
		String clusterName = "george-cluster";
		falconUtils.deleteClusterEntity(clusterName);		

	}		
	
	@Test
	public void velocitize() throws Exception {
		String value = falconUtils.velocitizeClusterConfig("falcon/entities/cluster/primary-cluster.xml");
		System.out.println(value);
		assertNotNull(value);
		Assert.assertFalse(value.contains("hdfs.url"));
	}
	
	
	@Test
	public void loadEntityFile() throws Exception {
		String value = falconUtils.loadEntityFile("falcon/entities/feed/raw-data-feed.xml");
		assertNotNull(value);
		System.out.println(value);
	}	
	
	
	@Test
	public void testSubmitClusterEntity() throws Exception{
		String file = "/falcon/entities/cluster/primary-cluster.xml";
		falconUtils.submitClusterEntity(file);
	}
	
	@Test
	public void testDeleteClusterEntity() throws Exception{
		String clusterName = "george-cluster";
		falconUtils.deleteClusterEntity(clusterName);
	}	
	

	
	@Test
	public void testSubmitAndScheduleFeedEntity() throws Exception{
		
		// First submit the cluster entitity
		String cluster = "/falcon/entities/cluster/primary-cluster.xml";
		falconUtils.submitClusterEntity(cluster);
		
		//then test submiting the feed
		String feed = "falcon/entities/feed/raw-data-feed.xml";
		falconUtils.submitAndScheduleFeedEntity(feed);
	}	
	

	@Test
	public void testDeleteFeedEntity() throws Exception{
		String rawDataFeedName = "truck-events-raw-data-feed";
		falconUtils.deleteFeedEntity(rawDataFeedName);
	}	

	

	

}
