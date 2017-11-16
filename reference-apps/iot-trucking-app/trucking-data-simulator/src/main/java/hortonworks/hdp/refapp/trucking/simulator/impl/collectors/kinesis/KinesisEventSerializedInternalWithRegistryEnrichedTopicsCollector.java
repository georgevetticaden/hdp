package hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis;

import java.nio.ByteBuffer;

import hortonworks.hdp.refapp.trucking.simulator.impl.collectors.BaseSerializerTruckEventCollector;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.EventSourceType;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.MobileEyeEvent;
import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;


public class KinesisEventSerializedInternalWithRegistryEnrichedTopicsCollector extends BaseSerializerTruckEventCollector {


	private AmazonKinesis kinesisClient = null;
	private EventSourceType eventSourceType;

	public KinesisEventSerializedInternalWithRegistryEnrichedTopicsCollector(String regionName, EventSourceType eventSource, String schemaRegistryUrl) {
		super(schemaRegistryUrl);
		this.eventSourceType = eventSource;
 
        try {		
            kinesisClient = createKinesisClient(regionName);    
            validateStream(kinesisClient, TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_TOPIC_NAME );
            validateStream(kinesisClient, TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_TOPIC_NAME );

        } catch (Exception e) {
        	logger.error("Error creating Kinesis Client" , e);
        }      
	}
	
	static AmazonKinesis createKinesisClient(String regionName) {
		AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        
        clientBuilder.setRegion(regionName);
        clientBuilder.setCredentials(new DefaultAWSCredentialsProviderChain());
        return clientBuilder.build();
        
	}
	
	static void validateStream(AmazonKinesis kClient, String streamName) {
        try {
            DescribeStreamResult result = kClient.describeStream(streamName);
            if(!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                String errMsg = "Stream " + streamName + " is not active. Please wait a few moments and try again.";
                throw new RuntimeException(errMsg);
            }
        } catch (ResourceNotFoundException e) {
            String errMsg = "Stream " + streamName + " does not exist. Please create it in the console.";
            throw new RuntimeException(errMsg, e);
        } catch (Exception e) {
            String errMsg = "Error found while describing the stream " + streamName;
            throw new RuntimeException(errMsg, e);
        }
    }	

	@Override
	public void onReceive(Object event) throws Exception {

		MobileEyeEvent mee = (MobileEyeEvent) event;
		
		if(eventSourceType == null || EventSourceType.ALL_STREAMS.equals(eventSourceType)) {
			sendTruckEventToKinesis(mee);	
			sendTruckSpeedEventToKinesis(mee);	
		} else if(EventSourceType.GEO_EVENT_STREAM.equals(eventSourceType)) {
			sendTruckEventToKinesis(mee);	
		} else if (EventSourceType.SPEED_STREAM.equals(eventSourceType)) {	
			sendTruckSpeedEventToKinesis(mee);
		}		
	}
	
	
	private void sendTruckEventToKinesis(MobileEyeEvent mee) throws Exception {
		
		
		byte[] serializedPayload = serializeTruckGeoEvent(mee);
    	logger.debug("Creating serialized truck geo event["+serializedPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");

		
		PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_TOPIC_NAME);
        
        // We use the driverId as the partition key
        putRecord.setPartitionKey(String.valueOf(mee.getTruck().getDriver().getDriverId()));
        putRecord.setData(ByteBuffer.wrap(serializedPayload));

        try {
            kinesisClient.putRecord(putRecord);
        } catch (AmazonClientException ex) {
			logger.error("Error sending serialized geo event[" + serializedPayload + "] to  Kinesis stream["+TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_TOPIC_NAME +"]", ex);
        }		
		
		
	}
		
	
	private void sendTruckSpeedEventToKinesis(MobileEyeEvent mee) throws Exception {

		byte[] serializedPayload = serializeTruckSpeedEvent(mee);
		logger.debug("Creating serialized truck speed event["+serializedPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			
	
		PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_TOPIC_NAME);
        
        // We use the driverId as the partition key
        putRecord.setPartitionKey(String.valueOf(mee.getTruck().getDriver().getDriverId()));
        putRecord.setData(ByteBuffer.wrap(serializedPayload));
        
        try {
            kinesisClient.putRecord(putRecord);
        } catch (AmazonClientException ex) {
			logger.error("Error sending serialized speed event[" + serializedPayload + "] to  Kafka topic["+TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_TOPIC_NAME +"]", ex);
        }		
		        
	

	}

}
