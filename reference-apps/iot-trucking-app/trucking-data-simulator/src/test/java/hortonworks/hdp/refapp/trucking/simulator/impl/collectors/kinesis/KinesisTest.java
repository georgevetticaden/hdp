package hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis;

import java.util.logging.Logger;
import org.apache.commons.logging.Log;

import hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis.consumer.TruckGeoEventRecordProcessor;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.EventSourceType;
import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

public class KinesisTest extends BaseKinesisTest {
	
	protected Log LOG = LogFactory.getLog(KinesisTest.class);

	@Test
	public void testKinesisConnection() {
		//LOG.info("About to test Kinesis connection");
		AmazonKinesis kinesisClinet = createKinesisClient();
	}
	
	
	
	
	
}
