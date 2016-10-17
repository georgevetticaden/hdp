package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaProvider;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

public class TruckSchemaRegistryLoader {
	
	
	private static final String TRUCK_SPEED_EVENTS_SCHEMA_NAME = "truck_speed_events_avro";
	private static final String TRUCK_EVENTS_SCHEMA_NAME = "truck_events_avro";
	private static final String SCHEMA_GROUP_NAME = "truck-sensors";
	private static final String AVRO_SERIALIZER_NAME = "avro-serializer";
	private static final String AVRO_DESERIALIZER_NAME = "avro-deserializer";
	
	protected Logger LOG = LoggerFactory.getLogger(TruckSchemaRegistryLoader.class);
	
	private SchemaRegistryClient schemaRegistryClient;
	
	
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
	
	public void loadSchemaRegistry() {
		try {
			registerSchemaMetaDataForTruckGeoEvent();
			registerSchemaMetaDataForTruckSpeedEvent();
			addSchemaForTruckGeoEvent();
			addSchemaForTruckSpeedEvent();
			uploadAndRegisterAndMapSeDeserializers();
		} catch (Exception e) {
			String errorMsg = "Error loading data into Schema Registry for truck events";
			LOG.error(errorMsg, e);
			throw new RuntimeException(e);
		}		
	}
	
	
	public void uploadAndRegisterAndMapSeDeserializers() throws Exception {
		 String avroserdesJarName = "/schema/schema-registry-avro-0.1.0-SNAPSHOT.jar";
	        
		 InputStream serdesJarInputStream = TruckSchemaRegistryLoader.class.getResourceAsStream(avroserdesJarName);
	        
		 if (serdesJarInputStream == null) {       
			 Assert.fail("Jar " + avroserdesJarName + " could not be loaded");
	     }
	        
		 //upload
		 String fileId = schemaRegistryClient.uploadFile(serdesJarInputStream);
		 
		 //register
		 long serializerId = registerAvroSerializer(fileId);
		 long deserializerId =  registerAvroDeserializer(fileId);
		 
		 //map truck geo event
	     SchemaMetadata schemaMetadata = createSchemaMetaForTruckGeoEvent();
		 String schemaName = schemaMetadata.getName();
	     schemaRegistryClient.mapSchemaWithSerDes(schemaName, serializerId);
	     schemaRegistryClient.mapSchemaWithSerDes(schemaName, deserializerId);	
	     
	     //map for truck speed event
	     SchemaMetadata truckSpeedSchemaMetadata = createSchemaMetaForTruckSpeedEvent();
		 String truckSpeedSchema = truckSpeedSchemaMetadata.getName();
	     schemaRegistryClient.mapSchemaWithSerDes(truckSpeedSchema, serializerId);
	     schemaRegistryClient.mapSchemaWithSerDes(truckSpeedSchema, deserializerId);		     
		 
	}
	
	public void addSchemaForTruckGeoEvent() throws Exception {
		  
		String schemaFileName = "/schema/truck-geo-event.avsc";
		String schema = getSchema(schemaFileName);
		LOG.info("Truck Geo Event Schema is: " + schema);
		
		SchemaVersion schemaVersion = new SchemaVersion(schema, "Initial Schema for Truck Geo Event");
		int version = schemaRegistryClient.addSchemaVersion(createSchemaMetaForTruckGeoEvent().getName(), schemaVersion);
		LOG.info("Version Id of new schema is: " + version);
		
		
	}	
	
	public void addSchemaForTruckSpeedEvent() throws Exception {
		  
		String schemaFileName = "/schema//truck-speed-event.avsc";
		String schema = getSchema(schemaFileName);
		LOG.info("Truck Speed Schema is: " + schema);
		
		SchemaVersion schemaVersion = new SchemaVersion(schema, "Initial Schema for Truck Speed Event");
		int version = schemaRegistryClient.addSchemaVersion(createSchemaMetaForTruckSpeedEvent().getName(), schemaVersion);
		LOG.info("Version Id of new schema is: " + version);
		
		
	}	
	
	
	
	private void registerSchemaMetaDataForTruckGeoEvent() throws Exception {
		  
		SchemaMetadata schemaMetadata = createSchemaMetaForTruckGeoEvent();
		// register the schemaGroup
	    boolean status = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
	    LOG.info("Status of registering schema is: " + status);
	}
	
	private void registerSchemaMetaDataForTruckSpeedEvent() throws Exception {
		  
		SchemaMetadata schemaMetadata = createSchemaMetaForTruckSpeedEvent();
		// register the schemaGroup
	    boolean status = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
	    LOG.info("Status of registering schema is: " + status);
	}	
	
	private SchemaMetadata createSchemaMetaForTruckGeoEvent() {
		String schemaGroup = SCHEMA_GROUP_NAME;
		String schemaName = TRUCK_EVENTS_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Geo events from trucks";
		SchemaProvider.Compatibility compatiblity = SchemaProvider.Compatibility.BACKWARD;
		
		SchemaMetadata schemaMetadata = createSchemaMetadata(schemaGroup, schemaName, schemaType, description, compatiblity);
		return schemaMetadata;
	}
	
	private SchemaMetadata createSchemaMetaForTruckSpeedEvent() {
		String schemaGroup = SCHEMA_GROUP_NAME;
		String schemaName = TRUCK_SPEED_EVENTS_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Speed Events from trucks";
		SchemaProvider.Compatibility compatiblity = SchemaProvider.Compatibility.BACKWARD;
		
		SchemaMetadata schemaMetadata = createSchemaMetadata(schemaGroup, schemaName, schemaType, description, compatiblity);
		return schemaMetadata;
	}	
	
	
	
    private SchemaMetadata createSchemaMetadata(String schemaGroup, String schemaName, String schemaType, String description, SchemaProvider.Compatibility compatiblity) {
        return new SchemaMetadata.Builder(schemaName)
                .type(schemaType)
                .schemaGroup(schemaGroup)
                .description(description)
                .compatibility(compatiblity)
                .build();
    }	
    
    private Long registerAvroSerializer(String fileId) {
        String avroSerializerClassName = "com.hortonworks.registries.schemaregistry.avro.AvroSnapshotSerializer";
        SerDesInfo serializerInfo = new SerDesInfo.Builder()
                .name(AVRO_SERIALIZER_NAME)
                .description("The Default Avro Serializer")
                .fileId(fileId)
                .className(avroSerializerClassName)
                .buildSerializerInfo();
        return schemaRegistryClient.addSerializer(serializerInfo);
    }

    private Long registerAvroDeserializer(String fileId) {
        String avroDeserializerClassName = "com.hortonworks.registries.schemaregistry.avro.AvroSnapshotDeserializer";
        SerDesInfo deserializerInfo = new SerDesInfo.Builder()
                .name("avro-deserializer")
                .description("The Default Avro Deserializer")
                .fileId(fileId)
                .className(avroDeserializerClassName)
                .buildDeserializerInfo();
        return schemaRegistryClient.addDeserializer(deserializerInfo);
    }      
    
    private String getSchema(String schemaFileName) throws IOException {
        InputStream schemaResourceStream = TruckSchemaRegistryLoader.class.getResourceAsStream(schemaFileName);
        if (schemaResourceStream == null) {
            throw new IllegalArgumentException("Given schema file [" + schemaFileName + "] does not exist");
        }

        return IOUtils.toString(schemaResourceStream, "UTF-8");
    }      
	
    private static Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Options.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
        config.put(SchemaRegistryClient.Options.CLASSLOADER_CACHE_SIZE, 10);
        config.put(SchemaRegistryClient.Options.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS, 5000L);
        config.put(SchemaRegistryClient.Options.SCHEMA_CACHE_SIZE, 1000);
        config.put(SchemaRegistryClient.Options.SCHEMA_CACHE_EXPIRY_INTERVAL_SECS, 60 * 60 * 1000L);
        return config;
    }		
		

}
