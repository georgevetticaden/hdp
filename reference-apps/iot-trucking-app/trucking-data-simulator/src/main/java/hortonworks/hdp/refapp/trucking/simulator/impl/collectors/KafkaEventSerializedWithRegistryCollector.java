package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.EventSourceType;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;
import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import java.util.Properties;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;


public class KafkaEventSerializedWithRegistryCollector extends BaseSerializerTruckEventCollector {

	
	private KafkaProducer<String, Object> kafkaProducer;
	private EventSourceType eventSourceType;

	public KafkaEventSerializedWithRegistryCollector(String kafkaBrokerList, EventSourceType eventSource, String schemaRegistryUrl) {
		super(schemaRegistryUrl);
		this.eventSourceType = eventSource;
        Properties props = configureKafkaProps(kafkaBrokerList, schemaRegistryUrl);        
 
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

	private void sendTruckSpeedEventToKafka(MobileEyeEvent mee) throws Exception {

		final Callback callback = new MyProducerCallback();
		Object truckSpeedEvent = createGenericRecordForTruckSpeedEvent("/schema/truck-speed-event-log.avsc", mee);
	
		logger.debug("Creating Avro truck speed event["+ReflectionToStringBuilder.toString(truckSpeedEvent)+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			
	
		try {
			ProducerRecord<String, Object> data = new ProducerRecord<String, Object>(TruckSchemaConfig.KAFKA_RAW_TRUCK_SPEED_EVENT_SCHEMA_NAME, truckSpeedEvent);
			kafkaProducer.send(data);			
		} catch (Exception e) {
			logger.error("Error sending event[" + truckSpeedEvent + "] to Kafka topic", e);
		}		
		
	}
	

	private void sendTruckEventToKafka(MobileEyeEvent mee) throws Exception {
		
		Object truckGeoEvent = createGenericRecordForTruckGeoEvent("/schema/truck-geo-event-log.avsc", mee);
		
		logger.debug("Creating Avro truck geo event["+ReflectionToStringBuilder.toString(truckGeoEvent)+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			

		try {
			ProducerRecord<String, Object> data = new ProducerRecord<String, Object>(TruckSchemaConfig.KAFKA_RAW_TRUCK_GEO_EVENT_SCHEMA_NAME, truckGeoEvent);
			kafkaProducer.send(data);			
		} catch (Exception e) {
			logger.error("Error sending AVro Object [" + truckGeoEvent + "] to Kafka topic", e);
		}
	}
	
	private Properties configureKafkaProps(String kafkaBrokerList, String schemaRegistryUrl) {
		Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerList);

        props.put("request.required.acks", "1");
        
        // producer configuration
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);

        // consumer configuration
      //  props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL, schemaRegistryUrl);
       // props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());        
        
//        props.put("key.serializer", 
//                "org.apache.kafka.common.serialization.StringSerializer");
//                
//             props.put("value.serializer", 
//                "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}	
	
	 private  class MyProducerCallback implements Callback {
	        @Override
	        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	            logger.info("#### received [{}], ex: [{}]", recordMetadata, e);
	        }
	    }	

}
