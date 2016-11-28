package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.registries.schemaregistry.SchemaCompatibility;
import org.apache.registries.schemaregistry.SchemaIdVersion;
import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.SchemaVersion;
import org.apache.registries.schemaregistry.SerDesInfo;
import org.apache.registries.schemaregistry.avro.AvroSchemaProvider;
import org.apache.registries.schemaregistry.client.SchemaRegistryClient;

public class TruckSchemaRegistryLoader {
	

	
	protected Logger LOG = LoggerFactory.getLogger(TruckSchemaRegistryLoader.class);
	
	SchemaRegistryClient schemaRegistryClient;
	
	
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
		 String avroserdesJarName = "/schema/schema-registry-serdes-0.1.0.jar";
	        
		 InputStream serdesJarInputStream = TruckSchemaRegistryLoader.class.getResourceAsStream(avroserdesJarName);
	        
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
		SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(createSchemaMetaForTruckGeoEvent().getName(), schemaVersion);
		LOG.info("Version Id of new schema is: " + version);
		
		
	}	
	
	public void addSchemaForTruckSpeedEvent() throws Exception {
		  
		String schemaFileName = "/schema//truck-speed-event.avsc";
		String schema = getSchema(schemaFileName);
		LOG.info("Truck Speed Schema is: " + schema);
		
		SchemaVersion schemaVersion = new SchemaVersion(schema, "Initial Schema for Truck Speed Event");
		SchemaIdVersion version = schemaRegistryClient.addSchemaVersion(createSchemaMetaForTruckSpeedEvent().getName(), schemaVersion);
		LOG.info("Version Id of new schema is: " + version);
		
		
	}	
	
	
	
	private void registerSchemaMetaDataForTruckGeoEvent() throws Exception {
		  
		SchemaMetadata schemaMetadata = createSchemaMetaForTruckGeoEvent();
		// register the schemaGroup
	    long status = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
	    LOG.info("Status of registering schema is: " + status);
	}
	
	private void registerSchemaMetaDataForTruckSpeedEvent() throws Exception {
		  
		SchemaMetadata schemaMetadata = createSchemaMetaForTruckSpeedEvent();
		// register the schemaGroup
	    long status = schemaRegistryClient.registerSchemaMetadata(schemaMetadata);
	    LOG.info("Status of registering schema is: " + status);
	}	
	
	SchemaMetadata createSchemaMetaForTruckGeoEvent() {
		String schemaGroup = TruckSchemaConfig.SCHEMA_GROUP_NAME;
		String schemaName = TruckSchemaConfig.TRUCK_EVENTS_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Geo events from trucks";
		SchemaCompatibility compatiblity = SchemaCompatibility.BACKWARD;
		
		SchemaMetadata schemaMetadata = createSchemaMetadata(schemaGroup, schemaName, schemaType, description, compatiblity);
		return schemaMetadata;
	}
	
	SchemaMetadata createSchemaMetaForTruckSpeedEvent() {
		String schemaGroup = TruckSchemaConfig.SCHEMA_GROUP_NAME;
		String schemaName = TruckSchemaConfig.TRUCK_SPEED_EVENTS_SCHEMA_NAME;
		String schemaType = AvroSchemaProvider.TYPE;
		String description = "Speed Events from trucks";
		SchemaCompatibility compatiblity = SchemaCompatibility.BACKWARD;
		
		SchemaMetadata schemaMetadata = createSchemaMetadata(schemaGroup, schemaName, schemaType, description, compatiblity);
		return schemaMetadata;
	}	
	
	
	
    SchemaMetadata createSchemaMetadata(String schemaGroup, String schemaName, String schemaType, String description, SchemaCompatibility compatiblity) {
        return new SchemaMetadata.Builder(schemaName)
                .type(schemaType)
                .schemaGroup(schemaGroup)
                .description(description)
                .compatibility(compatiblity)
                .build();
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
