package hortonworks.hdp.apputil.registry;

import java.util.List;
import java.util.Map;


/**
 * Registry for Common Endpoints (Host Names, Ports, etc..) exposed by various HDP Components (Hive, HDFS, HBase, Kafka, etc)
 * Also any custom endpoint property is supported that is not part of Ambari (Solr, Ranger)
 * Traditional Endpoints are discovered via:
 * 	1. Config file
 *  2. Ambari REST calls
 *  3. Slider REST Calls
 *  
 *  Custom Endpoints (e.g: SOLR): are dsicovered via a config file
 *  
 * @author gvetticaden
 *
 */
public interface HDPServiceRegistry {

	/**
	 * Returns the HBase Zoookeper Host 
	 * Source: Config, Ambari or Slider
	 * Prop Key: hbase.zookeeper.host
	 * E.g: centralregion01.cloud.hortonworks.com
	 * 
	 * @return
	 */
	String getHBaseZookeeperHost() ;
	
	/**
	 * Returns the HBase Zookeeper client Port
	 * Source: Config, Ambari or Slider
	 * Prop Key: hbase.zookeeper.client.port
	 * E.g: 2181
	 * 
	 * @return
	 */
	String getHBaseZookeeperClientPort() ;

	/**
	 * Returns the HBase Zookeeper ZNode Parent
	 * Source: Config, Ambari or Slider
	 * Prop Key: hbase.zookeeper.znode.parent
	 * E.g: /services/slider/users/yarn/hbase-on-yarn-v35
	 * 
	 * @return
	 */	
	String getHBaseZookeeperZNodeParent() ;
	
	/**
	 * Returns the Phoenix Connection URL
	 * Source: Derived fro HBase Information
	 * Prop Key: phoenix.connection.url
	 * E.g: jdbc:phoenix:centralregion01.cloud.hortonworks.com:2181:/services/slider/users/yarn/hbase-on-yarn-v35
	 * 
	 * @return
	 */	
	String getPhoenixConnectionURL() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getStormZookeeperQuorum() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getStormNimbusPort() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getStormNimbusHost() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getKafkaBrokerList() ;	
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	List<String> getStormZookeeperQuorumAsList();	
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getKafkaZookeeperHost() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getKafkaZookeeperClientPort() ;	
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String  getKafkaZookeeperZNodeParent() ;		
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getHDFSUrl() ;
	
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getHiveMetaStoreUrl();	
	
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getHiveServer2ConnectionURL() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getFalconServerUrl() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getFalconBrokerUrl() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getFalconServerPort() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getAmbariServerUrl() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getOozieUrl() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getClusterName() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getResourceManagerURL() ;
	
	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getResourceManagerUIURL() ;

	/**
	 * Returns the
	 * Source: Config, Ambari or Slider
	 * Prop Key: 
	 * E.g: 
	 * 
	 * @return
	 */	
	String getStormUIUrl();
	
	/**
	 * Returns custom value that the registry doesn't provide a value for.
	 * @param key
	 * @return
	 */
	String getCustomValue(String key);
	
	
	/**
	 * Returns the entire registry
	 * @return
	 */
	Map<String, String> getRegistry();

	/**
	 * Populate the Registry based on the params passed in and any custom params
	 * @param params
	 * @throws Exception
	 */
	void populate(ServiceRegistryParams params, Map<String, String> customParams, String configFileName) throws Exception;
	
	/**
	 * Populate the Registry based on the params passed in
	 * @param params
	 * @throws Exception
	 */
	void populate(ServiceRegistryParams params) throws Exception;
		
	
	/**
	 * Populates teh registry based on picking up params from the config file
	 * @throws Exception
	 */
	void populate() throws Exception;
	
	/**
	 * Writes teh registry to a properties config file passed in initially
	 * @throws Exception
	 */
	void writeToPropertiesFile() throws Exception;	

}
