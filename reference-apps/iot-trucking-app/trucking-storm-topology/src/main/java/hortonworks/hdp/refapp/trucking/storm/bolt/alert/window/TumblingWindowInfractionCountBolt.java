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


public class TumblingWindowInfractionCountBolt extends BaseWindowedBolt {


	private static final long serialVersionUID = 8257758116690028179L;
	private static final Logger LOG = LoggerFactory.getLogger(TumblingWindowInfractionCountBolt.class);
	
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }	
	
	@Override
	public void execute(TupleWindow inputWindow) {
		
		
		Map<TruckDriver, TruckDriverInfractionDetail> truckDriverInfractions = new HashMap<TruckDriver, TruckDriverInfractionDetail>();
			
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
			
			
			if(isInfractionEvent(eventType)) {
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Infraction Event["+eventType+"] detected and will be counted");
				}
				
				TruckDriver driver = new TruckDriver(driverId, driverName, truckId, routeName);
				TruckDriverInfractionDetail infractionDetail = truckDriverInfractions.get(driver);
				if(infractionDetail == null) {
					infractionDetail = new TruckDriverInfractionDetail(driver);
					truckDriverInfractions.put(driver, infractionDetail);
				}
				infractionDetail.addInfraction(eventType);
			}
			collector.ack(tuple);
		}
		
		
		if(LOG.isInfoEnabled()) {
			LOG.info("About to output result from Infraction Count Tumbling Window");
			LOG.info("-----Total Number of Driver Infractions from Output Window is" + truckDriverInfractions.size());
			for(TruckDriver truckDriver: truckDriverInfractions.keySet()) {
				LOG.info(truckDriverInfractions.get(truckDriver).toString());
			}
			LOG.info("-----End of Total Number of Driver Infractions from Output Window is" + truckDriverInfractions.size());
		}
		
		
		collector.emit(new Values(truckDriverInfractions));

	}

	
	private boolean isInfractionEvent(String eventType) {
		return !"Normal".equals(eventType);
	}
	
	 @Override
	 public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        
		 declarer.declare(new Fields("driversInfractionDetails"));
	    
	 }	

	


}
