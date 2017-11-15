package hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis.consumer;

import java.util.UUID;
import java.util.logging.Logger;

import hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis.BaseKinesisTest;
import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.Test;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class KinesisConsumerTest extends BaseKinesisTest {
	
	private static final String APP_NAME = "TruckEventsSAMProcessor";
	protected Logger LOG = Logger.getLogger(KinesisConsumerTest.class.getName());


	@Test
	public void consumeForTruckGeoEventStream() {
		
		LOG.info("Starting Consume Test..");
		Worker worker = createKinesisConsumerWorker(APP_NAME, TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_TOPIC_NAME);

        try {
            worker.run();
        } catch (Throwable t) {
        	
            LOG.info("Caught throwable while processing data. " + t);
        }
	}

	public Worker createKinesisConsumerWorker(String appName, String streamName) {
		
		String workerId = String.valueOf(UUID.randomUUID());
		KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration(appName, streamName, new DefaultAWSCredentialsProviderChain(), workerId)
            .withRegionName(REGION_NAME);		
		
		IRecordProcessorFactory recordProcessorFactory = new TruckGeoEventProcessor();
		Worker worker = new Worker(recordProcessorFactory, kclConfig);
		return worker;
	}	

}
