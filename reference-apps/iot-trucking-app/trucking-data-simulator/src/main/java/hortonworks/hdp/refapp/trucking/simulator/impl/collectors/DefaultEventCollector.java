package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.AbstractEventCollector;


public class DefaultEventCollector extends AbstractEventCollector {

	
	@Override
	public void onReceive(Object message) throws Exception {
		logger.info("on receive", message);
	}


}
