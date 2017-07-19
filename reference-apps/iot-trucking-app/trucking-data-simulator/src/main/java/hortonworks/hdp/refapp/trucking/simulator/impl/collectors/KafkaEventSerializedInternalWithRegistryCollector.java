package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.SecurityType;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.EventSourceType;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;
import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class KafkaEventSerializedInternalWithRegistryCollector extends BaseSerializerTruckEventCollector {


	private KafkaProducer<String, Object> kafkaProducer;
	private EventSourceType eventSourceType;

	public KafkaEventSerializedInternalWithRegistryCollector(String kafkaBrokerList, EventSourceType eventSource, String schemaRegistryUrl, SecurityType securityType) {
		super(schemaRegistryUrl);
		this.eventSourceType = eventSource;
        Properties props = configureKafkaProps(kafkaBrokerList, schemaRegistryUrl, securityType);        
 
        try {		
            kafkaProducer = new KafkaProducer<String, Object>(props);        	
        } catch (Exception e) {
        	logger.error("Error creating producer" , e);
        }      
	}
	
	@Override
	public void onReceive(Object event) throws Exception {

		MobileEyeEvent mee = (MobileEyeEvent) event;
		
		if(eventSourceType == null || EventSourceType.ALL_STREAMS.equals(eventSourceType)) {
			sendTruckEventToKafka(mee);	
			sendTruckSpeedEventToKafka(mee);	
		} else if(EventSourceType.GEO_EVENT_STREAM.equals(eventSourceType)) {
			sendTruckEventToKafka(mee);	
		} else if (EventSourceType.SPEED_STREAM.equals(eventSourceType)) {	
			sendTruckSpeedEventToKafka(mee);
		}		
	}
	
	
	private void sendTruckEventToKafka(MobileEyeEvent mee) throws Exception {
		
		final Callback callback = new MyProducerCallback();
		
		byte[] serializedPayload = serializeTruckGeoEvent(mee);
		
		logger.debug("Creating serialized truck geo event["+serializedPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			
		
		
		try {
			ProducerRecord<String, Object> data = new ProducerRecord<String, Object>(TruckSchemaConfig.KAFKA_RAW_TRUCK_EVENT_TOPIC_NAME, serializedPayload);
			kafkaProducer.send(data, callback);			
		} catch (Exception e) {
			logger.error("Error sending serialized geo event[" + serializedPayload + "] to  Kafka topic["+TruckSchemaConfig.KAFKA_RAW_TRUCK_EVENT_TOPIC_NAME +"]", e);
		}
	}
		
	
	private void sendTruckSpeedEventToKafka(MobileEyeEvent mee) throws Exception {

		final Callback callback = new MyProducerCallback();
		byte[] serializedPayload = serializeTruckSpeedEvent(mee);
		logger.debug("Creating serialized truck speed event["+serializedPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			
	
	
		try {
			ProducerRecord<String, Object> data = new ProducerRecord<String, Object>(TruckSchemaConfig.KAFKA_RAW_TRUCK_EVENT_TOPIC_NAME, serializedPayload);
			kafkaProducer.send(data, callback);			
		} catch (Exception e) {
			logger.error("Error sending serialized speed event[" + serializedPayload + "] to  Kafka topic["+TruckSchemaConfig.KAFKA_RAW_TRUCK_EVENT_TOPIC_NAME +"]", e);

		}		
		
	}
	

	
	private Properties configureKafkaProps(String kafkaBrokerList,
			String schemaRegistryUrl, SecurityType securityType) {
		 Properties props = new Properties();
	     props.put("bootstrap.servers", kafkaBrokerList);
	     props.put("request.required.acks", "1");
	     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	     props.put("value.serializer", 
	                "org.apache.kafka.common.serialization.ByteArraySerializer");  
	     
	     return props;
	}	
	
	
	 private  class MyProducerCallback implements Callback {
	        @Override
	        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	           // logger.info("#### received [{}], ex: [{}]", recordMetadata, e);
	        }
	    }		

}
