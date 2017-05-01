package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.EventSourceType;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;
import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;


public class FileEventWithSchemaIdCollector extends BaseTruckEventCollector {


	private static final String LINE_BREAK = "\n";
	
	private static final String SCHEMA_ID_DELIMITER = "<schema-id>";
	private static final String SCHEMA_VERSION_DELIMITER = "<schema-version>";

	
	private File truckEventsFile;

	private EventSourceType eventSourceType;

	private SchemaRegistryClient schemaRegistryClient;

	private long geoEventSchemaId;

	private long speedSchemaId;

	

	public FileEventWithSchemaIdCollector(String fileName,EventSourceType eventSource, String schemaRegistryUrl) {
       this.truckEventsFile = new File(fileName);
       this.eventSourceType = eventSource;
       this.schemaRegistryClient = new SchemaRegistryClient(createConfig(schemaRegistryUrl));
       
       try {
    	   this.geoEventSchemaId = getSchemaId(TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_NAME);
		   this.speedSchemaId = getSchemaId(TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_NAME);
		   
		   logger.info("Schema Id for geo event schema is: " + geoEventSchemaId);
		   logger.info("Schema Id for speed schema is: " + speedSchemaId);
       } catch (Exception e) {
    	   throw new RuntimeException("Error getting schemaIds from Schema Registry");

       }
	}
      
	
	
	@Override
	public void onReceive(Object event) throws Exception {
		MobileEyeEvent mee = (MobileEyeEvent) event;
		
		if(eventSourceType == null || EventSourceType.ALL_STREAMS.equals(eventSourceType)) {
			sendTruckEventToFile(mee);	
			sendTruckSpeedEventToFile(mee);			
		} else if(EventSourceType.GEO_EVENT_STREAM.equals(eventSourceType)) {
			sendTruckEventToFile(mee);
		} else if (EventSourceType.SPEED_STREAM.equals(eventSourceType)) {
			sendTruckSpeedEventToFile(mee);	
		}

	}

	private void sendTruckEventToFile(MobileEyeEvent mee) {
		
		String eventToPass = createTruckGeoEvent(mee) +"|" + LINE_BREAK;
		String eventToPassWithSchema = addSchemaInfo(geoEventSchemaId, 
											TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_VERSION, eventToPass);
		logger.debug("Creating truck geo event["+eventToPassWithSchema+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");
		
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPassWithSchema, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPassWithSchema + "] to file[ " + truckEventsFile + " ] ", e);
		}		
		
	}



	private void sendTruckSpeedEventToFile(MobileEyeEvent mee) {
		
		String eventToPass = createTruckSpeedEvent(mee) + "|" + LINE_BREAK;
		String eventToPassWithSchema = addSchemaInfo(speedSchemaId, 
											TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_VERSION, eventToPass);
		logger.debug("Creating speed event["+eventToPassWithSchema+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");	
		
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPassWithSchema, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPassWithSchema + "] to file[ " + truckEventsFile + " ] ", e);
		}	
	}

	
	private String addSchemaInfo(long schemaId, int schemaVersion, String event) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(SCHEMA_ID_DELIMITER)
			  .append(schemaId)
			  .append(SCHEMA_ID_DELIMITER)
			  .append(SCHEMA_VERSION_DELIMITER)
			  .append(schemaVersion)
			  .append(SCHEMA_VERSION_DELIMITER)
			  .append("|")
			  .append(event);
		return buffer.toString();
	}	
	
    private Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        
        return config;
    }		
		
	private long getSchemaId(String schemaName) throws Exception {
		  

	    SchemaMetadataInfo metaInfo = getSchemaMetaData(schemaName);
	    long schemaId = metaInfo.getId();
	    return schemaId;
	}
	
	private SchemaMetadataInfo getSchemaMetaData(String schemaName) {
		SchemaMetadataInfo metaInfo= schemaRegistryClient.getSchemaMetadataInfo(schemaName);
		
		return metaInfo;
	}	
	
}
