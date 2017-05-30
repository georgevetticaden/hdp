package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.SecurityType;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;

public abstract class BaseKafkaSRSerializerTruckEventCollector extends
		BaseSerializerTruckEventCollector {

	public BaseKafkaSRSerializerTruckEventCollector(String schemaRegistryUrl) {
		super(schemaRegistryUrl);
		// TODO Auto-generated constructor stub
	}
	
	protected Properties configureKafkaProps(String kafkaBrokerList, String schemaRegistryUrl, SecurityType securityType) {
		Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokerList);

        props.put("request.required.acks", "1");
        
        /* Configure to use the Schema REgistry Serializer */
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        
        /* If talking to secure Kafka cluster, set security protocol as "SASL_PLAINTEXT */
        if(SecurityType.SECURE.equals(securityType))
        	props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

		return props;
	}		



}
