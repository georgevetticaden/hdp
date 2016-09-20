package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.AbstractEventCollector;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;


public class FileEventCollector extends AbstractEventCollector {


	private static final String LINE_BREAK = "\n";
	private File truckEventsFile;

	public FileEventCollector(String fileName) {
       this.truckEventsFile = new File(fileName);
      
	}
	
	@Override
	public void onReceive(Object event) throws Exception {
		MobileEyeEvent mee = (MobileEyeEvent) event;
		createTruckEvent(mee);	
		createTruckSpeedEvent(mee);

	}

	private void createTruckEvent(MobileEyeEvent mee) {
		String driverId = String.valueOf(mee.getTruck().getDriver().getDriverId());
		
		
		String eventToPass = "DIVIDER" + mee.getTruck().toString() + mee.getTruckSpeed() +"|" + LINE_BREAK;
		logger.debug("Creating truck speed event["+eventToPass+"] for driver["+driverId + "] in truck [" + mee.getTruck() + "]");
		
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPass, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to file[ " + truckEventsFile + " ] ", e);
		}		
		
	}

	private void createTruckSpeedEvent(MobileEyeEvent mee) {
		String eventToPass = "DIVIDER" + mee.toString() + LINE_BREAK;
		String driverId = String.valueOf(mee.getTruck().getDriver().getDriverId());
		
		logger.debug("Creating event["+eventToPass+"] for driver["+driverId + "] in truck [" + mee.getTruck() + "]");
		
		try {
			FileUtils.writeStringToFile(truckEventsFile, eventToPass, Charset.defaultCharset(), true);
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to file[ " + truckEventsFile + " ] ", e);
		}	
	}

}
