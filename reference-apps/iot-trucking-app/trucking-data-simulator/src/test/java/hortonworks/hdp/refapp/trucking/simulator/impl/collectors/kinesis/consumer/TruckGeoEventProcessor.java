package hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis.consumer;

import java.util.logging.Logger;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class TruckGeoEventProcessor implements IRecordProcessorFactory {

	protected Logger LOG = Logger.getLogger(TruckGeoEventProcessor.class.getName());

	@Override
	public IRecordProcessor createProcessor() {
		LOG.info("Creating instance of GeoEventRecordProcessor...");
		return new TruckGeoEventRecordProcessor();
	}

}
