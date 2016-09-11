package hortonworks.hdp.refapp.trucking.simulator.masters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hortonworks.hdp.refapp.trucking.simulator.impl.messages.EmitEvent;
import hortonworks.hdp.refapp.trucking.simulator.impl.messages.StartSimulation;
import hortonworks.hdp.refapp.trucking.simulator.impl.messages.StopSimulation;
import hortonworks.hdp.refapp.trucking.simulator.results.SimulationResultsSummary;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinRouter;


@SuppressWarnings("rawtypes")
public class SimulationMaster extends UntypedActor {
	private int numberOfEventEmitters = 1;
	private int numberOfEvents = 1;
	private Class eventEmitterClass;
	private ActorRef eventEmitterRouter;
	private ActorRef listener;
	private int eventCount = 0;
	private Logger logger = LoggerFactory.getLogger(SimulationMaster.class);
	private long delay_between_trucks;
	
	@SuppressWarnings("unchecked")
	public SimulationMaster(int numberOfEventEmitters, Class eventEmitterClass,
			ActorRef listener, int numberOfEvents, long demoId, int messageDelay) {
		logger.info("Starting simulation with " + numberOfEventEmitters
				+ " of " + eventEmitterClass + " Event Emitters -- "
				+ eventEmitterClass.toString());
		this.listener = listener;
		this.numberOfEventEmitters = numberOfEventEmitters;
		this.eventEmitterClass = eventEmitterClass;
		this.delay_between_trucks = messageDelay;
		this.numberOfEvents = numberOfEvents;
		eventEmitterRouter = this.getContext().actorOf(
				Props.create(eventEmitterClass, numberOfEvents, demoId, messageDelay).withRouter(
						new RoundRobinRouter(numberOfEventEmitters)),
				"eventEmitterRouter");
		
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof StartSimulation) {
			logger.info("Starting Simulation");
			
			int emitter_counter = 0;
			while (emitter_counter < numberOfEventEmitters) {
				
				eventEmitterRouter.tell(new EmitEvent(), getSelf());
				emitter_counter++;
			}
				
		} else if (message instanceof StopSimulation) {
			listener.tell(new SimulationResultsSummary(eventCount), getSelf());
//			this.getContext().system().shutdown();
//			System.exit(0);
		} else {
			logger.debug("Received message I'm not sure what to do with: "
					+ message);
		}
	}
}