package hortonworks.hdp.refapp.trucking.simulator.listeners;

import hortonworks.hdp.refapp.trucking.simulator.results.SimulationResultsSummary;
import akka.actor.UntypedActor;


public class SimulatorListener extends UntypedActor {

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof SimulationResultsSummary)
			System.out.println(message.toString());
//		getContext().system().shutdown();
	}
}
