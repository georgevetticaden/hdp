package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

public  final class TruckSchemaConfig {
	
	/* Streamline kafka source always appends :v when doing a lookup of a schema via the kakfa topic name */
	public static final String SCHEMA_NAME_SUFFIX=":v";
	
	/* Schema Group Name */
	public static final String SCHEMA_GROUP_NAME = "truck-sensors";
	
	/* Schema names for the two streams of data */
	public static final String TRUCK_SPEED_EVENTS_SCHEMA_NAME = "truck_speed_events_avro" + SCHEMA_NAME_SUFFIX;
	public static final String TRUCK_EVENTS_SCHEMA_NAME = "truck_events_avro" + SCHEMA_NAME_SUFFIX;
	
	/* Serializer Names */
	public static final String AVRO_SERIALIZER_NAME = "avro-serializer";
	public static final String AVRO_DESERIALIZER_NAME = "avro-deserializer";
	
	
	/* Versions for each of the schemas */
	public static final int TRUCK_GEO_EVENT_SCHEMA_VERSION = 1;
	public static final int TRUCK_SPEED_EVENT_SCHEMA_VERSION = 1;	

}
