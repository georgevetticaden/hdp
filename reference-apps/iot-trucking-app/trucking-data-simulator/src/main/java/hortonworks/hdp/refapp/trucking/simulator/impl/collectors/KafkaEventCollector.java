package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.AbstractEventCollector;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;

import java.util.Properties;

import kafka.producer.KeyedMessage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;




public class KafkaEventCollector extends AbstractEventCollector {

	private static final String TRUCK_EVENT_TOPIC = "truck_events";
	private static final String TRUCK_SPEED_EVENT_TOPIC = "truck_speed_events";
	
	private KafkaProducer<String, String> kafkaProducer;

	public KafkaEventCollector(String kafkaBrokerList) {
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
		String driverId = String.valueOf(mee.getTruck().getDriver().getDriverId());
		
		String eventToPass = "DIVIDER" + mee.getTruck().toString() + mee.getTruckSpeed() +"|";
		logger.debug("Creating truck speed event["+eventToPass+"] for driver["+driverId + "] in truck [" + mee.getTruck() + "]");
		
		try {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(TRUCK_SPEED_EVENT_TOPIC, driverId, eventToPass);
			kafkaProducer.send(data);			
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to Kafka topic", e);
		}		
		
	}

	private void sendTruckEventToKafka(MobileEyeEvent mee) {
		String eventToPass = "DIVIDER" + mee.toString();
		String driverId = String.valueOf(mee.getTruck().getDriver().getDriverId());
		
		logger.debug("Creating event["+eventToPass+"] for driver["+driverId + "] in truck [" + mee.getTruck() + "]");
		
		try {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(TRUCK_EVENT_TOPIC, driverId, eventToPass);
			kafkaProducer.send(data);			
		} catch (Exception e) {
			logger.error("Error sending event[" + eventToPass + "] to Kafka topic", e);
		}
	}

}
