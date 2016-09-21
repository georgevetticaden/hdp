package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;


public class FileEventCollector extends BaseTruckEventCollector {


	private static final String LINE_BREAK = "\n";
	private File truckEventsFile;

	public FileEventCollector(String fileName) {
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
			
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPass, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to file[ " + truckEventsFile + " ] ", e);
		}		
		
	}

	private void sendTruckSpeedEventToFile(MobileEyeEvent mee) {
		
		String eventToPass = createTruckSpeedEvent(mee) + "|" + LINE_BREAK;
		
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPass, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to file[ " + truckEventsFile + " ] ", e);
		}	
	}

}
