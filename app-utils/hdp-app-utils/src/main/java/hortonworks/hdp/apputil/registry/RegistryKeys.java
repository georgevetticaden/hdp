package hortonworks.hdp.apputil.registry;



public class RegistryKeys {
	
	// Key for Location of Director where Registry Config is
	public static final String SERVICE_REGISTRY_CONFIG_LOCATION_SYSTEM_PROP_KEY = "service.registry.config.location";
	public static final String SERVICE_REGISTRY_CONFIG_LOCATION_IS_ABSOLUTE_PROP_KEY = "service.registry.config.location.absolute.path";
	
	// Valid values are STANDALONE or SLIDER
	public static final String HBASE_DEPLOYMENT_MODE = "hbase.deployment.mode";
	public static final String HBASE_ZOOKEEPER_HOST = "hbase.zookeeper.host";
	public static final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.client.port";
	public static final String HBASE_ZOOKEEPER_ZNODE_PARENT = "hbase.zookeeper.znode.parent";
	public static final String HBASE_SLIDER_PUBLISHER_URL = "hbase.slider.publisher.url";
	
	public static final String PHOENIX_CONNECTION_URL = "phoenix.connection.url";
	
	// Valid values are STANDALONE or SLIDER
	public static final String STORM_DEPLOYMENT_MODE = "storm.deployment.mode";	
	public static final String STORM_ZOOKEEPER_QUORUM = "storm.zookeeper.quorum";
	public static final String STORM_NIMBUS_PORT = "storm.nimbus.port";
	public static final String STORM_NIMBUS_HOST = "storm.nimbus.host";
	public static final String STORM_SLIDER_PUBLISHER_URL = "storm.slider.publisher.url";
	
	public static final String KAFKA_BROKER_LIST = "kafka.broker.list";	
	public static final String KAFKA_ZOOKEEPER_HOST = "kafka.zookeeper.host";
	public static final String KAFKA_ZOOKEEPER_CLIENT_PORT = "kafka.zookeeper.client.port";
	public static final String KAFKA_ZOOKEEPER_ZNODE_PARENT = "kafka.zookeeper.znode.parent";
	
	public static final String HDFS_URL = "hdfs.url";
	
	public static final String HIVE_METASTORE_URL = "hive.metastore.url";
	public static final String HIVE_SERVER2_CONNECT_URL = "hiveserver2.connect.string";
	
	public static final String FALCON_SERVER_URL = "falcon.server.url";
	public static final String FALCON_BROKER_URL = "falcon.broker.url";
	public static final String FALCON_SERVER_PORT = "falcon.server.default.port";
	
	public static final String AMBARI_SERVER_URL = "ambari.server.url";
	public static final String AMBARI_CLUSTER_NAME = "ambari.cluster.name";

	public static final String RESOURCE_MANGER_URI_URL = "resourcemanager.ui.url";
	public static final String RESOURCE_MANGER_URL = "resourcemanager.url";
	
	
	public static final String STORM_UI_HOST = "storm.ui.host";
	public static final String STORM_UI_PORT = "storm.ui.port";
	
	public static final String OOZIE_SERVER_URL = "oozie.server.url";

	

	

	

	

}
