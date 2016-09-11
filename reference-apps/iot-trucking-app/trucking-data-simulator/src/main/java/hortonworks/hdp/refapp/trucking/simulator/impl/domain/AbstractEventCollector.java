package hortonworks.hdp.refapp.trucking.simulator.impl.domain;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.UntypedActor;

public abstract class AbstractEventCollector extends UntypedActor {

	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	public AbstractEventCollector() {
	}


}
