package hortonworks.hdp.apputil.kafka;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.log4j.Logger;

import scala.collection.Map;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;

public class KafkaUtils {

	
	private static final Logger LOG = Logger.getLogger(KafkaUtils.class);
	
	private HDPServiceRegistry serviceRegistry;

	
	public KafkaUtils(HDPServiceRegistry serviceRegistry) {
		super();
		this.serviceRegistry = serviceRegistry;
	}

	public void createTopic(String topicName, int replicationFactor, int numPartitions) {
		ZkClient client = createZookepperClient();
		Properties topologyConfig = new Properties();
		AdminUtils.createTopic(client, topicName, numPartitions, replicationFactor, topologyConfig);
		
	}

	public void deleteTopic(String topicName) {
		ZkClient client = createZookepperClient();
		
		client.deleteRecursive(ZkUtils.getTopicPath(topicName));
	}
	
	public Set<String> getTopics() {
		ZkClient zClient = createZookepperClient();
		Map<String, Properties> topicsScala = AdminUtils.fetchAllTopicConfigs(zClient);
		java.util.Map<String, Properties> topics = JavaConversions.asJavaMap(topicsScala);
		
		Set<String> topicNames = new HashSet<String>();
		for(String key: topics.keySet()) {
			topicNames.add(key);
		}
		return topicNames;
	}
	
	private ZkClient createZookepperClient() {
		String zookeeperHostPort = serviceRegistry.getKafkaZookeeperHost() + ":" + serviceRegistry.getKafkaZookeeperClientPort();
		
		int sessionTimeOut = 10000;
		int connectionTimeOut = 10000;
		ZkSerializer zkSerializer = ZKStringSerializer$.MODULE$;
		ZkClient client = new ZkClient(zookeeperHostPort, sessionTimeOut, connectionTimeOut, zkSerializer);
		return client;
	}	
	
	
	
}
