package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;
import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;


public class FileEventWithSchemaInfoCollector extends BaseTruckEventCollector {


	private static final String LINE_BREAK = "\n";
	
	private static final String SCHEMA_GROUP_DELIMITER = "<schema-group>";
	private static final String SCHEMA_NAME_DELIMITER = "<schema-name>";
	private static final String SCHEMA_VERSION_DELIMITER = "<schema-version>";

	
	private File truckEventsFile;

	public FileEventWithSchemaInfoCollector(String fileName) {
       this.truckEventsFile = new File(fileName);
      
	}
	
	@Override
	public void onReceive(Object event) throws Exception {
		MobileEyeEvent mee = (MobileEyeEvent) event;
		sendTruckEventToFile(mee);	
		sendTruckSpeedEventToFile(mee);

	}

	private void sendTruckEventToFile(MobileEyeEvent mee) {
		
		String eventToPass = createTruckGeoEvent(mee) +"|" + LINE_BREAK;
		String eventToPassWithSchema = addSchemaInfo(TruckSchemaConfig.SCHEMA_GROUP_NAME, TruckSchemaConfig.TRUCK_EVENTS_SCHEMA_NAME, TruckSchemaConfig.TRUCK_GEO_EVENT_SCHEMA_VERSION, eventToPass);
		logger.debug("Creating truck speed event["+eventToPassWithSchema+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");
		
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPassWithSchema, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPassWithSchema + "] to file[ " + truckEventsFile + " ] ", e);
		}		
		
	}



	private void sendTruckSpeedEventToFile(MobileEyeEvent mee) {
		
		String eventToPass = createTruckSpeedEvent(mee) + "|" + LINE_BREAK;
		String eventToPassWithSchema = addSchemaInfo(TruckSchemaConfig.SCHEMA_GROUP_NAME, TruckSchemaConfig.TRUCK_SPEED_EVENTS_SCHEMA_NAME, TruckSchemaConfig.TRUCK_SPEED_EVENT_SCHEMA_VERSION, eventToPass);
		logger.debug("Creating truck event["+eventToPassWithSchema+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");	
		
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPassWithSchema, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPassWithSchema + "] to file[ " + truckEventsFile + " ] ", e);
		}	
	}

	
	private String addSchemaInfo(String schemaGroup, String schemaName, int schemaVersion, String event) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(SCHEMA_GROUP_DELIMITER)
			  .append(schemaGroup)
			  .append(SCHEMA_GROUP_DELIMITER)
			  .append(SCHEMA_NAME_DELIMITER)
			  .append(schemaName)
			  .append(SCHEMA_NAME_DELIMITER)
			  .append(SCHEMA_VERSION_DELIMITER)
			  .append(schemaVersion)
			  .append(SCHEMA_VERSION_DELIMITER)
			  .append("|")
			  .append(event);
		return buffer.toString();
	}	
		
	
}
