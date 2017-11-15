package hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis;

import org.apache.commons.configuration.ConfigurationUtils;

import hortonworks.hdp.refapp.trucking.simulator.schemaregistry.TruckSchemaConfig;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public abstract class BaseKinesisTest {

	public static final String REGION_NAME = "us-west-2";

	
	protected AmazonKinesis createKinesisClient() {
		AmazonKinesis kinesisClinet = KinesisEventSerializedInternalWithRegistryEnrichedTopicsCollector.createKinesisClient(REGION_NAME);
		KinesisEventSerializedInternalWithRegistryEnrichedTopicsCollector.validateStream(kinesisClinet, TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_TOPIC_NAME);
		KinesisEventSerializedInternalWithRegistryEnrichedTopicsCollector.validateStream(kinesisClinet, TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_TOPIC_NAME);
		return kinesisClinet;
	}
	

}


