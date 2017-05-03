package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.EventSourceType;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;

import java.io.File;

import org.apache.commons.io.FileUtils;


public class FileEventSerializedWithRegistryCollector extends BaseSerializerTruckEventCollector {


	private static final String LINE_BREAK = "\n";
	private byte[] LINE_BREAK_BYTES = LINE_BREAK.getBytes();
	

	private File truckEventsFile;
	private EventSourceType eventSourceType;

	public FileEventSerializedWithRegistryCollector(String fileName, EventSourceType eventSource,  String schemaRegistryUrl) {
	   super(schemaRegistryUrl);
       this.truckEventsFile = new File(fileName);
       this.eventSourceType = eventSource;
       logger.info("Using Schema Registry["+schemaRegistryUrl+"] to serialize events");
      
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

	private void sendTruckEventToFile(MobileEyeEvent mee) throws Exception {
		
		byte[] serializedPayload = serializeTruckGeoEvent(mee);
		
		logger.debug("Creating serialized truck geo event["+serializedPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			
		
		try {
			FileUtils.writeByteArrayToFile(truckEventsFile, serializedPayload, true);
			//FileUtils.writeByteArrayToFile(truckEventsFile, LINE_BREAK_BYTES, true);
			
			
		} catch (Exception e) {
			logger.error("Error sending serialized event[" + serializedPayload + "] to file[ " + truckEventsFile + " ] ", e);
		}		
		
	}

	private void sendTruckSpeedEventToFile(MobileEyeEvent mee) throws Exception {

		byte[] serializedPayload = serializeTruckSpeedEvent(mee);
		logger.debug("Creating serialized truck speed event["+serializedPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			
		
		try {
			FileUtils.writeByteArrayToFile(truckEventsFile, serializedPayload, true);
			//FileUtils.writeByteArrayToFile(truckEventsFile, LINE_BREAK_BYTES, true);
		} catch (Exception e) {
			logger.error("Error sending serialized event[" + serializedPayload + "] to file[ " + truckEventsFile + " ] ", e);
		}	
	}
	

}
