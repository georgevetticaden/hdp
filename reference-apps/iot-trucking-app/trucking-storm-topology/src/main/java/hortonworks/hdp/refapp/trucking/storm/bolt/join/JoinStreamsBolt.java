package hortonworks.hdp.refapp.trucking.storm.bolt.join;

import hortonworks.hdp.refapp.trucking.domain.TruckDriver;

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


public class JoinStreamsBolt extends BaseWindowedBolt {


	private static final long serialVersionUID = 8257758116690028179L;
	private static final Logger LOG = LoggerFactory.getLogger(JoinStreamsBolt.class);
	
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }	
	
    
    /**
     * Join TruckEvent with TruckSpeedEvents grouped by driverId, truckId and time
     */
	@Override
	public void execute(TupleWindow inputWindow) {
		
		List<Tuple> tuplesInWindow = inputWindow.get();
		LOG.debug("Events in Current Window: " + tuplesInWindow.size());
		
		Map<TruckDriver, List<TruckEvent>> truckDriversTruckEvents = new HashMap<TruckDriver, List<TruckEvent>>();
		
		Map<TruckDriver, List<TruckSpeedEvent>> truckDriversTruckSpeedEvents = new HashMap<TruckDriver, List<TruckSpeedEvent>>();
		
		/* Iterate through each of the tuples from teh 2 sources and group by truck and driver */
		for(Tuple tuple: tuplesInWindow) {
			
			int truckId = tuple.getIntegerByField("truckId");
			int driverId = tuple.getIntegerByField("driverId");
			
			TruckDriver truckDriver = new TruckDriver(driverId, null, truckId, null );
			
			String sourceComponent = tuple.getSourceComponent();
			if("Truck-Events-Stream".equals(sourceComponent)) {
				List<TruckEvent> driverTruckEvents = truckDriversTruckEvents.get(truckDriver);
				if(driverTruckEvents == null) {
					driverTruckEvents = new ArrayList<JoinStreamsBolt.TruckEvent>();
					truckDriversTruckEvents.put(truckDriver, driverTruckEvents);
				}
				driverTruckEvents.add(createTruckEvent(tuple));
			} else if("Truck-Speed-Events-Stream".equals(sourceComponent)) {
				List<TruckSpeedEvent> driverTruckSpeedEvents = truckDriversTruckSpeedEvents.get(truckDriver);
				if(driverTruckSpeedEvents == null) {
					driverTruckSpeedEvents = new ArrayList<JoinStreamsBolt.TruckSpeedEvent>();
					truckDriversTruckSpeedEvents.put(truckDriver, driverTruckSpeedEvents);
				}			
				driverTruckSpeedEvents.add(createTruckSpeedEvent(tuple));
			} else {
				LOG.info("Got a Tuple["+tuple+"] from unidentified source");
			}
			collector.ack(tuple);
		}
		
		/* Join by truck, driver and time */
		join(truckDriversTruckEvents, truckDriversTruckSpeedEvents);
		
		/* Left outer join has occurred, now emit all the values */
		for(TruckDriver truckDriver: truckDriversTruckEvents.keySet()) {
			List<TruckEvent> truckDriverEvents = truckDriversTruckEvents.get(truckDriver);
			for(TruckEvent truckEvent: truckDriverEvents) {
				Values values = new Values(truckEvent.driverId, truckEvent.truckId, truckEvent.eventTime, 
										truckEvent.eventType, truckEvent.speed, truckEvent.longitude,
										truckEvent.latitude, truckEvent.eventKey, truckEvent.correlationId, 
										truckEvent.driverName, truckEvent.routeId, truckEvent.routeName);
				collector.emit(values);
			}
						
		}

	}
	

	
	private void join(
			Map<TruckDriver, List<TruckEvent>> truckDriversTruckEvents,
			Map<TruckDriver, List<TruckSpeedEvent>> truckDriversTruckSpeedEvents) {
		
		
		for(TruckDriver truckDriver: truckDriversTruckEvents.keySet()) {
			
			List<TruckEvent> driverTruckEvents = truckDriversTruckEvents.get(truckDriver);
			List<TruckSpeedEvent> driverTruckSpeedEvents = truckDriversTruckSpeedEvents.get(truckDriver);
			
			if(driverTruckSpeedEvents != null && !driverTruckSpeedEvents.isEmpty()) {
				for(TruckEvent truckDriverEvent: driverTruckEvents) {
					
					TruckSpeedEvent matchedTruckSpeedEvent = findMatchingTruckSpeedEvent(truckDriverEvent, driverTruckSpeedEvents);
					if(matchedTruckSpeedEvent != null) {
						join(truckDriverEvent, matchedTruckSpeedEvent);
						LOG.info("Joined TruckEvent["+truckDriverEvent+"] with TruckSpeedEvent["+matchedTruckSpeedEvent+"]");
					} else {
						LOG.debug("No Matching TruckSpeedEVent found for Truck Event["+ truckDriverEvent.toString()+"]");
					}
				}				
			} else {
				LOG.debug("There were no speed events for Driver["+truckDriver+"] found in this window. So no join will occur");
			}

		}
	}

	private void join(TruckEvent truckDriverEvent,
			TruckSpeedEvent matchedTruckSpeedEvent) {
		truckDriverEvent.speed = matchedTruckSpeedEvent.speed;
	}

	/**
	 * Find the TruckSpeed Event for the TruckEvent grouped by truckId, driverId and time
	 * @param truckDriverEvent
	 * @param driverTruckSpeedEvents
	 * @return
	 */
	private TruckSpeedEvent findMatchingTruckSpeedEvent(TruckEvent truckDriverEvent,
			List<TruckSpeedEvent> driverTruckSpeedEvents) {

		TruckSpeedEvent closestSpeedEvent = null;
		Timestamp eventTime = truckDriverEvent.eventTime;
		long eventTimeLong = eventTime.getTime();
		
		long smallestDiff = Long.MAX_VALUE;
		int index = -1;
		for(int i=0;i<driverTruckSpeedEvents.size();i++) {
			Timestamp speedEventTime = driverTruckSpeedEvents.get(i).eventTime;
			long diff = Math.abs(speedEventTime.getTime() - eventTimeLong);
			if(diff < smallestDiff) {
				smallestDiff = diff;
				index = i;
			}
		}
		
		if(index != -1) {
			closestSpeedEvent = driverTruckSpeedEvents.remove(index);
		} 
		return closestSpeedEvent;
		
	}

	private TruckSpeedEvent createTruckSpeedEvent(Tuple input) {
		// TODO Auto-generated method stub
		
		Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
		int truckId = input.getIntegerByField("truckId");
		int driverId = input.getIntegerByField("driverId");
		int truckSpeed = input.getIntegerByField("truckSpeed");
		
		TruckSpeedEvent speedEvent = new TruckSpeedEvent(driverId, truckId, eventTime, truckSpeed);
		
		if(LOG.isTraceEnabled()) {
			LOG.trace("Created TruckSpeedEvent Object["+speedEvent+"]");
		}
		
		return speedEvent;
	}

	private TruckEvent createTruckEvent(Tuple input) {

		Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
		int driverId = input.getIntegerByField("driverId");
		String driverName = input.getStringByField("driverName");
		int truckId = input.getIntegerByField("truckId");
		int routeId = input.getIntegerByField("routeId");
		String routeName = input.getStringByField("routeName");
		
		
		double longitude = input.getDoubleByField("longitude");
		double latitude = input.getDoubleByField("latitude");
		String eventType = input.getStringByField("eventType");
		
		long correlationId = input.getLongByField("correlationId");
		String eventKey = input.getStringByField("eventKey");
			
		
		TruckEvent truckEvent = new TruckEvent(driverId, truckId, eventTime, null, eventType, longitude, latitude, driverName, routeId, routeName, correlationId, eventKey);
		
		if(LOG.isTraceEnabled()) {
			LOG.trace("Created TruckEvent Object["+truckEvent+"]");	
		}
		
		
		return truckEvent;
	
	}

	
	 @Override
	 public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        
		declarer.declare(new Fields("driverId", "truckId", "eventTime", "eventType", "truckSpeed", 
									"longitude","latitude", "eventKey", "correlationId", 
									"driverName", "routeId", "routeName"));
	    
	 }	

	 
	 private static class  TruckSpeedEvent {
		 
		 int driverId;
		 int truckId;
		 Timestamp eventTime;
		 Integer speed;
		 
		public TruckSpeedEvent(int driverId, int truckId, Timestamp eventTime, Integer speed) {
			super();
			this.driverId = driverId;
			this.truckId = truckId;
			this.eventTime = eventTime;
			this.speed = speed;
		}
		 
		@Override
		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append("eventTime:" + eventTime);
			buffer.append("|");
			buffer.append("driverId=" + driverId);
			buffer.append("|");
			buffer.append("truckId=" + truckId);
			buffer.append("|");
			buffer.append("speed="+ speed);
			return buffer.toString();
		}
		 

	 }
	 
	 private static class TruckEvent extends TruckSpeedEvent {
		 
		public long correlationId;
		public String eventKey;
		String eventType;
		double longitude;
		double latitude;
		String driverName;
		int routeId ;
		String routeName;
		
		public TruckEvent(int driverId, int truckId, Timestamp eventTime,
				Integer speed, String eventType, double longitude, double latitude,
				String driverName, int routeId, String routeName, long correlationId, String eventKey) {
			super(driverId, truckId, eventTime, speed);
			this.eventType = eventType;
			this.longitude = longitude;
			this.latitude = latitude;
			this.driverName = driverName;
			this.routeId = routeId;
			this.routeName = routeName;
			this.correlationId = correlationId;
			this.eventKey = eventKey;
		}	 
		
		@Override
		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append("eventTime:" + eventTime);
			buffer.append("|");	
			buffer.append("driverName=" + driverName);
			buffer.append("|");
			buffer.append("driverId=" + driverId);
			buffer.append("|");
			buffer.append("truckId=" + truckId);
			buffer.append("|");
			buffer.append("routeName=" + routeName);
			buffer.append("|");	
			buffer.append("routeId=" + routeId);
			buffer.append("|");			
			buffer.append("latitude="+ latitude);
			buffer.append("|");
			buffer.append("longitude="+ longitude);
			buffer.append("|");
			buffer.append("correlationId="+ correlationId);
			buffer.append("|");			
			buffer.append("eventKey="+ eventKey);
			buffer.append("|");		
			buffer.append("eventType="+ eventType);
			buffer.append("|");		
			buffer.append("speed="+ speed);
			return buffer.toString();
		}
		
		
	 }
	


}
