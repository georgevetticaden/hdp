package hortonworks.hdp.refapp.trucking.storm.bolt.alert.window;

import hortonworks.hdp.refapp.trucking.domain.TruckDriver;
import hortonworks.hdp.refapp.trucking.domain.TruckDriverInfractionDetail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SlidingWindowAverageSpeedBolt extends BaseWindowedBolt {


	private static final long serialVersionUID = 8257758116690028179L;
	private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowAverageSpeedBolt.class);
	
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }	
	
	@Override
	public void execute(TupleWindow inputWindow) {
		
		
		Map<TruckDriver, TruckDriverSpeeds> truckDriversSpeeds = new HashMap<TruckDriver, TruckDriverSpeeds>();
			
		List<Tuple> tuplesInWindow = inputWindow.get();
		LOG.debug("Events in Current Window: " + tuplesInWindow.size());
		
		for(Tuple tuple: tuplesInWindow) {
			int driverId = tuple.getIntegerByField("driverId");
			String driverName = tuple.getStringByField("driverName");
			int routeId = tuple.getIntegerByField("routeId");
			String routeName = tuple.getStringByField("routeName");
			int truckId = tuple.getIntegerByField("truckId");
			Timestamp eventTime = (Timestamp) tuple.getValueByField("eventTime");
			String eventType = tuple.getStringByField("eventType");
			double longitude = tuple.getDoubleByField("longitude");
			double latitude = tuple.getDoubleByField("latitude");
			long correlationId = tuple.getLongByField("correlationId");	
			Object speedObject = tuple.getValueByField("truckSpeed");
			int speed = 0;
			if(speedObject != null) {
				speed = ((Integer)speedObject).intValue();
			} else {
				LOG.info("Tuple["+ tuple +"] has no speed" );
			}
			 
			
			TruckDriver driver = new TruckDriver(driverId, driverName, truckId, routeName);
			TruckDriverSpeeds truckDriverSpeeds = truckDriversSpeeds.get(driver);
			if(truckDriverSpeeds == null) {
				truckDriverSpeeds = new TruckDriverSpeeds(driver);
				truckDriversSpeeds.put(driver, truckDriverSpeeds);
			}
			truckDriverSpeeds.addSpeed(speed);
			collector.ack(tuple);
		}

		for(TruckDriver truckDriver: truckDriversSpeeds.keySet()) {
			TruckDriverSpeeds truckSpeeds = truckDriversSpeeds.get(truckDriver);
			int avergeSpeed = truckSpeeds.calculateAverageSpeed();
			LOG.info("TruckDriver["+ truckDriver +"] average speed is: " + avergeSpeed);
			collector.emit(new Values(truckDriver.getDriverId(), truckDriver, avergeSpeed));
		}

	}
	
	 @Override
	 public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        
		 declarer.declare(new Fields("driverId", "truckDriver", "averageSpeed"));
	    
	 }	

	


}
