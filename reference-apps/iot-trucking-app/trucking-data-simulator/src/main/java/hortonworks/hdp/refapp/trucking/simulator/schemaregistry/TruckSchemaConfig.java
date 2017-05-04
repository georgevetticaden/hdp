package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

public  final class TruckSchemaConfig {
	
	/** ----------Common Schema Registry Meta Info ------------------- **/
	
	/* Serializer Names */
	//public static final String AVRO_SERDES_JAR_NAME = "/schema/schema-registry-serdes-0.1.0-SNAPSHOT.jar";
	public static final String AVRO_SERIALIZER_NAME = "avro-serializer";
	public static final String AVRO_DESERIALIZER_NAME = "avro-deserializer";	
	
	
	
	/** ----------- The following are schema meta info for the schema for the truck event log data -------------*/

	
	/* Schema Group Name */
	public static final String LOG_SCHEMA_GROUP_NAME = "truck-sensors-log";
	
	/* Schema names for the two streams of data */
	public static final String LOG_TRUCK_SPEED_EVENT_SCHEMA_NAME = "truck_speed_events_log";
	public static final String LOG_TRUCK_GEO_EVENT_SCHEMA_NAME = "truck_events_log";
	
	
	/* Versions for each of the schemas */
	public static final int LOG_TRUCK_GEO_EVENT_SCHEMA_VERSION = 1;
	public static final int LOG_TRUCK_SPEED_EVENT_SCHEMA_VERSION = 1;		
	
	
	/** ------------ The following are schema meta info for the schema for the truck event kafka topics ----------------*/
	
	/* :v indicates to SChema REgistry Kafka Deserializer that this is is the schema for the value as opposed to the key */
	public static final String KAFKA_SCHEMA_NAME_SUFFIX=":v";
	
	/* Schema Group Name */
	public static final String KAFKA_SCHEMA_GROUP_NAME = "truck-sensors-kafka";
	
	/* Schema names for the two streams of data */
	public static final String KAFKA_TRUCK_SPEED_EVENT_SCHEMA_NAME = "truck_speed_events_avro" + KAFKA_SCHEMA_NAME_SUFFIX;
	public static final String KAFKA_TRUCK_GEO_EVENT_SCHEMA_NAME = "truck_events_avro" + KAFKA_SCHEMA_NAME_SUFFIX;
	
	/* Kafka Topics for raw event.  Events are published to these topics if truck generator serializes truck events into kafka */
	public static final String KAFKA_RAW_TRUCK_SPEED_EVENT_TOPIC_NAME = "raw-truck_speed_events_avro";
	public static final String KAFKA_RAW_TRUCK_GEO_EVENT_TOPIC_NAME = "raw-truck_events_avro";
	
	public static final String KAFKA_RAW_TRUCK_SPEED_EVENT_SCHEMA_NAME = KAFKA_RAW_TRUCK_SPEED_EVENT_TOPIC_NAME + KAFKA_SCHEMA_NAME_SUFFIX;
	public static final String KAFKA_RAW_TRUCK_GEO_EVENT_SCHEMA_NAME = KAFKA_RAW_TRUCK_GEO_EVENT_TOPIC_NAME + KAFKA_SCHEMA_NAME_SUFFIX;
	
	/* Versions for each of the schemas */
	public static final int KAFKA_TRUCK_GEO_EVENT_SCHEMA_VERSION = 1;
	public static final int KAFKA_TRUCK_SPEED_EVENT_SCHEMA_VERSION = 1;	
	public static final int KAFKA_RAW_TRUCK_GEO_EVENT_SCHEMA_VERSION = 1;
	public static final int KAFKA_RAW_TRUCK_SPEED_EVENT_SCHEMA_VERSION = 1;	
	
	


}
