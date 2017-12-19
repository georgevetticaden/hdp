package hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis;

import java.nio.ByteBuffer;

import hortonworks.hdp.refapp.trucking.simulator.impl.collectors.BaseSerializerTruckEventCollector;
import hortonworks.hdp.refapp.trucking.simulator.impl.collectors.BaseTruckEventCollector;
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


public class KinesisEventCSVEnrichedTopicsCollector extends BaseTruckEventCollector {

	
	private static final String TRUCK_SPEED_EVENTS = "george_truck_speed_events";
	private static final String TRUCK_EVENTS = "truck_events";

	private AmazonKinesis kinesisClient = null;
	private EventSourceType eventSourceType;

	public KinesisEventCSVEnrichedTopicsCollector(String regionName, EventSourceType eventSource) {
		this.eventSourceType = eventSource;
 
        try {		
            kinesisClient = createKinesisClient(regionName);    
            validateStream(kinesisClient, TRUCK_EVENTS);
            validateStream(kinesisClient, TRUCK_SPEED_EVENTS );

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
		
		
		String csvPayload = createTruckGeoEvent(mee);
    	logger.debug("Creating serialized truck geo event["+csvPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");

		
		PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(TRUCK_EVENTS);
        
        // We use the driverId as the partition key
        putRecord.setPartitionKey(String.valueOf(mee.getTruck().getDriver().getDriverId()));

        putRecord.setData(ByteBuffer.wrap(csvPayload.getBytes()));

        try {
            kinesisClient.putRecord(putRecord);
        } catch (AmazonClientException ex) {
			logger.error("Error sending serialized geo event[" + csvPayload + "] to  Kinesis stream["+TRUCK_EVENTS +"]", ex);
        }		
		
		
	}
		
	
	private void sendTruckSpeedEventToKinesis(MobileEyeEvent mee) throws Exception {

		String csvPayload = createTruckSpeedEvent(mee);
		logger.debug("Creating serialized truck speed event["+csvPayload+"] for driver["+mee.getTruck().getDriver().getDriverId() + "] in truck [" + mee.getTruck() + "]");			
	
		PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(TRUCK_SPEED_EVENTS);
        
        // We use the driverId as the partition key
        putRecord.setPartitionKey(String.valueOf(mee.getTruck().getDriver().getDriverId()));
        putRecord.setData(ByteBuffer.wrap(csvPayload.getBytes()));
        
        try {
            kinesisClient.putRecord(putRecord);
        } catch (AmazonClientException ex) {
			logger.error("Error sending serialized speed event[" + csvPayload + "] to  Kafka topic["+TRUCK_SPEED_EVENTS +"]", ex);
        }		
		        
	

	}

}
