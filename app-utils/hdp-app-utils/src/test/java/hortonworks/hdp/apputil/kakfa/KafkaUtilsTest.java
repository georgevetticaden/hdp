package hortonworks.hdp.apputil.kakfa;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import hortonworks.hdp.apputil.BaseUtilsTest;
import hortonworks.hdp.apputil.kafka.KafkaUtils;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;

public class KafkaUtilsTest extends BaseUtilsTest {

	private KafkaUtils kafkaUtils;

	@Before
	public void initialize() throws Exception {
		HDPServiceRegistry serviceRegistry = createHDPServiceRegistryWithAmbariAndSliderParams(DEFAULT_CONFIG_FILE_NAME, false);
		this.kafkaUtils = new KafkaUtils(serviceRegistry);
	}
	
	@Test
	public void testCreateAndListAndDeleteTopic() {
		String topicName = "test_topic";
		
		//delete the topic first if it exists
		kafkaUtils.deleteTopic(topicName);
		
		//add the topic
		int replicationFactor = 1;
		int numPartitions = 2;
		kafkaUtils.createTopic(topicName, replicationFactor, numPartitions);
		
		//verify the topic got created
		Set<String> topicNames = kafkaUtils.getTopics();
		assertTrue(topicNames.contains(topicName));
		
		
		//delete the topic adn verifiy it was deleted
		kafkaUtils.deleteTopic(topicName);
		topicNames = kafkaUtils.getTopics();
		assertFalse(topicNames.contains(topicName));		
		
	}
	
	@Test
	public void testDeleteTopic() {
		String topicName = "document_events_2";
		
		//delete the topic first if it exists
		kafkaUtils.deleteTopic(topicName);		
	}
	
	@Test
	public void testCreateTopic() {
		String topicName = "document_events_2";
		int replicationFactor = 1;
		int numPartitions = 2;
		kafkaUtils.createTopic(topicName, replicationFactor, numPartitions);		
	}
	
	
}
