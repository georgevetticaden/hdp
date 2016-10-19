package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.Truck;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;




public class KafkaJsonEventCollector extends BaseTruckEventCollector {

	private static final String TRUCK_EVENT_TOPIC = "truck_events_stream";
	private static final String TRUCK_SPEED_EVENT_TOPIC = "truck_speed_events_stream";
	
	private KafkaProducer<String, String> kafkaProducer;

	public KafkaJsonEventCollector(String kafkaBrokerList) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerList);

        props.put("request.required.acks", "1");
        
        props.put("key.serializer", 
                "org.apache.kafka.common.serialization.StringSerializer");
                
             props.put("value.serializer", 
                "org.apache.kafka.common.serialization.StringSerializer");        
 
        try {		
            kafkaProducer = new KafkaProducer<String, String>(props);        	
        } catch (Exception e) {
        	logger.error("Error creating producer" , e);
        }
        
      
	}
	
	@Override
	public void onReceive(Object event) throws Exception {
		MobileEyeEvent mee = (MobileEyeEvent) event;
		sendTruckEventToKafka(mee);	
		sendTruckSpeedEventToKafka(mee);

	}

	private void sendTruckSpeedEventToKafka(MobileEyeEvent mee) {
		
		String eventToPass = createTruckSpeedEventJsonString(mee);
		logger.debug("Creating truck speed event["+eventToPass+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");
		
		String driverId = String.valueOf(mee.getTruck().getDriver().getDriverId());		
		
		try {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(TRUCK_SPEED_EVENT_TOPIC, driverId, eventToPass);
			kafkaProducer.send(data);			
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to Kafka topic", e);
		}		
		
	}
	

	
	

	private void sendTruckEventToKafka(MobileEyeEvent mee) {
		String eventToPass = createTruckGeoEventJsonString(mee);
		logger.debug("Creating truck event["+eventToPass+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");	
		
		String driverId = String.valueOf(mee.getTruck().getDriver().getDriverId());
		
		try {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(TRUCK_EVENT_TOPIC, driverId, eventToPass);
			kafkaProducer.send(data);			
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to Kafka topic", e);
		}
	}

	
	private String createTruckSpeedEventJsonString(MobileEyeEvent mee) {
		
		
		
		TruckSpeedEvent speedEvent = new TruckSpeedEvent();
		speedEvent.setEventTime( new Timestamp(new Date().getTime()).toString());
		speedEvent.setEventSource("truck_speed_event");
		Truck truck = mee.getTruck();
		speedEvent.setTruckId(truck.getTruckId());
		speedEvent.setDriverId(truck.getDriver().getDriverId());;
		speedEvent.setDriverName(truck.getDriver().getDriverName());
		speedEvent.setRouteId(truck.getDriver().getRoute().getRouteId());
		speedEvent.setRoute(truck.getDriver().getRoute().getRouteName());
		
		speedEvent.setSpeed(mee.getTruckSpeed());
		
		String eventToPass = "";
		try {
			eventToPass =  new ObjectMapper().writeValueAsString(speedEvent);
		} catch (Exception e) {
			logger.error("error converting object to json" , e);
		}
		
		
		return eventToPass;
		
	}
	
	private String createTruckGeoEventJsonString(MobileEyeEvent mee) {
		
		
		
		TruckGeoEent geoEvent = new TruckGeoEent();
		geoEvent.setEventTime( new Timestamp(new Date().getTime()).toString());
		geoEvent.setEventSource("truck_geo_event");
		Truck truck = mee.getTruck();
		geoEvent.setTruckId(truck.getTruckId());
		geoEvent.setDriverId(truck.getDriver().getDriverId());;
		geoEvent.setDriverName(truck.getDriver().getDriverName());
		geoEvent.setRouteId(truck.getDriver().getRoute().getRouteId());
		geoEvent.setRoute(truck.getDriver().getRoute().getRouteName());
		
		geoEvent.setEventType(mee.getEventType().toString());
		geoEvent.setLongitude(mee.getLocation().getLongitude());
		geoEvent.setLatitude(mee.getLocation().getLatitude());
		geoEvent.setCorrelationId(mee.getCorrelationId());

		
		String eventToPass = "";
		try {
			eventToPass =  new ObjectMapper().writeValueAsString(geoEvent);
		} catch (Exception e) {
			logger.error("error converting object to json" , e);
		}
		return eventToPass;
		
	}	
	


}
