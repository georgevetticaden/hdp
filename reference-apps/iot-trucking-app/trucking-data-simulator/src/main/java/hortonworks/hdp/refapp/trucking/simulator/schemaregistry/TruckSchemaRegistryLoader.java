package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

public class TruckSchemaRegistryLoader {
	

	
	private static final Logger LOG = LoggerFactory.getLogger(TruckSchemaRegistryLoader.class);
	SchemaRegistryClient schemaRegistryClient;
	private Long serializerId;
	private Long deserializerId;
	
	
	public TruckSchemaRegistryLoader(String schmemaRegistryUrl) {
		this.schemaRegistryClient = new SchemaRegistryClient(createConfig(schmemaRegistryUrl));
	}

	public static void main(String args[]) {
		
		String schmemaRegistryUrl = args[0];
		if(StringUtils.isEmpty(schmemaRegistryUrl)) 
			throw new RuntimeException("Schema Registry REST URL must be provided");
		
		TruckSchemaRegistryLoader registryLoader = new TruckSchemaRegistryLoader(schmemaRegistryUrl);
		registryLoader.loadSchemaRegistry();
		
	}
	
	/**
	 * Loads the schema registry with all data required for the HDF Trucking Reference Application
	 */
	public void loadSchemaRegistry() {
		try {
			uploadAndRegisterSeDeserializers(TruckSchemaConfig.AVRO_SERDES_JAR_NAME);
			
			/* Populate the 2 schemas for the log files */
			populateSchemaRegistryForTruckGeoEventInLog();
			populateSchemaRegistryForTruckSpeedEventInLog();
			
			
			/* Populate the 2 schemas for the kafka topics */
			populateSchemaRegistryForTruckGeoEventInKafka();
			populateSchemaRegistryForTruckSpeedEventInKafka();
			
			
		} catch (Exception e) {
			String errorMsg = "Error loading data into Schema Registry for truck events";
			LOG.error(errorMsg, e);
			throw new RuntimeException(e);
		}		
	}


	private void populateSchemaRegistryForTruckGeoEventInLog() throws Exception {
		String schemaGroup = TruckSchemaConfig.LOG_SCHEMA_GROUP_NAME;
		String schemaName = TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Geo events from trucks in Log file";
		SchemaCompatibility compatiblity = SchemaCompatibility.BACKWARD;
		String schemaContentFileName = "/schema/truck-geo-event-log.avsc";
		
		registerSchemaMetaData(schemaGroup, schemaName, schemaType, description, compatiblity);
		addSchemaVersion(schemaName, schemaContentFileName);
		mapSeDeserializers(schemaName);
	}
	
	private void populateSchemaRegistryForTruckSpeedEventInLog() throws Exception {
		String schemaGroup = TruckSchemaConfig.LOG_SCHEMA_GROUP_NAME;
		String schemaName = TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Speed Events from trucks in Log File";
		SchemaCompatibility compatiblity = SchemaCompatibility.BACKWARD;
		String schemaContentFileName = "/schema/truck-speed-event-log.avsc";
		
		registerSchemaMetaData(schemaGroup, schemaName, schemaType, description, compatiblity);
		addSchemaVersion(schemaName, schemaContentFileName);
		mapSeDeserializers(schemaName);
	}	
	
	private void populateSchemaRegistryForTruckGeoEventInKafka() throws Exception {
		String schemaGroup = TruckSchemaConfig.KAFKA_SCHEMA_GROUP_NAME;
		String schemaName = TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Geo events from trucks in Kafka Topic";
		SchemaCompatibility compatiblity = SchemaCompatibility.BACKWARD;
		String schemaContentFileName = "/schema/truck-geo-event-kafka.avsc";
		
		registerSchemaMetaData(schemaGroup, schemaName, schemaType, description, compatiblity);
		addSchemaVersion(schemaName, schemaContentFileName);
		mapSeDeserializers(schemaName);
	}	
	
	private void populateSchemaRegistryForTruckSpeedEventInKafka() throws Exception {
		String schemaGroup = TruckSchemaConfig.KAFKA_SCHEMA_GROUP_NAME;
		String schemaName = TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Speed Events from trucks in Kafka Topic";
		SchemaCompatibility compatiblity = SchemaCompatibility.BACKWARD;
		String schemaContentFileName = "/schema/truck-speed-event-kafka.avsc";
		
		registerSchemaMetaData(schemaGroup, schemaName, schemaType, description, compatiblity);
		addSchemaVersion(schemaName, schemaContentFileName);
		mapSeDeserializers(schemaName);
	}	
	
	
	/**
	 * Upload the serdes jar that has the serializer and deserailizer classes and then registers teh serailizer and deserializer classes
	 * with Schema Registry
	 * @param avroserdesJarName
	 * @throws Exception
	 */
	public void uploadAndRegisterSeDeserializers(String avroserdesJarName) throws Exception {
		 
	    /* Uplaod the serializer jar to the schema registry */
		InputStream serdesJarInputStream = TruckSchemaRegistryLoader.class.getResourceAsStream(avroserdesJarName);
		String fileId = schemaRegistryClient.uploadFile(serdesJarInputStream);
		 
		 /* Register the Serializer and Deserializer classes in the uploaded jar with schema registry */
		 this.serializerId = registerAvroSerializer(fileId);
		 this.deserializerId =  registerAvroDeserializer(fileId);
	}
	
	/**
	 * Maps the registered serailizer and deserializer with the passed in schema
	 * @param schemaName
	 * @throws Exception
	 */
	public void mapSeDeserializers(String schemaName) throws Exception {

		 //map schema
	     schemaRegistryClient.mapSchemaWithSerDes(schemaName, serializerId);
	     schemaRegistryClient.mapSchemaWithSerDes(schemaName, deserializerId);			     
		 
	}	
	
	/**
	 * Adds a new schema version to a Schema
	 * @param schemaName
	 * @param schemaContentFileName
	 * @throws Exception
	 */
	public void addSchemaVersion(String schemaName, String schemaContentFileName) throws Exception {
		  
		String schema = getSchema(schemaContentFileName);
		LOG.info("Truck Geo Event Schema is: " + schema);
		
		SchemaVersion schemaVersion = new SchemaVersion(schema, "Initial Schema for Truck Geo Event");
		SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(schemaName, schemaVersion);
		LOG.info("Version Id of new schema is: " + version);
		
		
	}	

	
	
	/**
	 * Creates a New Schema Group and Schema Meta data construct in the Registry
	 * @param schemaGroup
	 * @param schemaName
	 * @param schemaType
	 * @param description
	 * @param compatiblity
	 * @throws Exception
	 */
	private void registerSchemaMetaData(String schemaGroup, String schemaName, String schemaType, 
		    String description, SchemaCompatibility compatiblity) throws Exception {
		  
		SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(schemaName)
														  .type(schemaType)
														  .schemaGroup(schemaGroup)
														  .description(description)
														  .compatibility(compatiblity)
														  .build();
		// register the schemaGroup
	    long status = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
	    LOG.info("Status of registering schema is: " + status);
	}
	
		
	
	
	
    private Long registerAvroSerializer(String fileId) {
        String avroSerializerClassName = "org.apache.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer";
        SerDesInfo serializerInfo = new SerDesInfo.Builder()
                .name(TruckSchemaConfig.AVRO_SERIALIZER_NAME)
                .description("The Default Avro Serializer")
                .fileId(fileId)
                .className(avroSerializerClassName)
                .buildSerializerInfo();
        return schemaRegistryClient.addSerializer(serializerInfo);
    }

    private Long registerAvroDeserializer(String fileId) {
        String avroDeserializerClassName = "org.apache.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer";
        SerDesInfo deserializerInfo = new SerDesInfo.Builder()
                .name("avro-deserializer")
                .description("The Default Avro Deserializer")
                .fileId(fileId)
                .className(avroDeserializerClassName)
                .buildDeserializerInfo();
        return schemaRegistryClient.addDeserializer(deserializerInfo);
    }      
    
    String getSchema(String schemaFileName) throws IOException {
        InputStream schemaResourceStream = TruckSchemaRegistryLoader.class.getResourceAsStream(schemaFileName);
        if (schemaResourceStream == null) {
            throw new IllegalArgumentException("Given schema file [" + schemaFileName + "] does not exist");
        }

        return IOUtils.toString(schemaResourceStream, "UTF-8");
    }      
	
    static Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        
        return config;
    }		
		
    
    

}
