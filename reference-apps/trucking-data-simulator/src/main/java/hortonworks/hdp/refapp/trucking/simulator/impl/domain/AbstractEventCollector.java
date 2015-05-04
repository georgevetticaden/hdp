package hortonworks.hdp.refapp.trucking.simulator.impl.domain;

import org.apache.log4j.Logger;

import akka.actor.UntypedActor;

public abstract class AbstractEventCollector extends UntypedActor {

	protected Logger logger = Logger.getLogger(this.getClass());

	public AbstractEventCollector() {
	}


}
