package hortonworks.hdp.refapp.trucking.simulator.impl.collectors.kinesis.consumer;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.apache.avro.generic.GenericRecord;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;

public class TruckGeoEventRecordProcessor implements IRecordProcessor {


	private static final String SCHEMA_REGISTRY_URL = "http://hdf-3-1-build3.field.hortonworks.com:7788/api/v1";
	
	protected Logger LOG = Logger.getLogger(TruckGeoEventRecordProcessor.class.getName());
	
	private SchemaRegistryClient schemaRegistryClient;

	private AvroSnapshotDeserializer deserializer;

	
	@Override
	public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        
        LOG.info("Creating Deserializer: " + shardId);

        schemaRegistryClient = new SchemaRegistryClient(createConfig(SCHEMA_REGISTRY_URL));
        
        deserializer = schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE);
	    deserializer.init(createConfig(SCHEMA_REGISTRY_URL));
	    Assert.assertNotNull(deserializer);   
	    
	    LOG.info("Successfully created client: " + shardId);

	}

	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		for (Record record : records) {
            // process record
            processTruckGeoEvent(record);
        }
	}

	private void processTruckGeoEvent(Record record) {

		LOG.info("About to process Kinesis Record[" + record + "]");
		
		InputStream serializedTruckEventStream =new ByteArrayInputStream(record.getData().array());

		Assert.assertNotNull(serializedTruckEventStream);
		
	
		Object avroTruckGeoEventRecord = deserializer.deserialize(serializedTruckEventStream, null);
		Assert.assertNotNull(avroTruckGeoEventRecord);
		
		GenericRecord genericRecord = (GenericRecord)avroTruckGeoEventRecord;
		LOG.info("I dont' trust toString, Longtitudde is: " + genericRecord.get("longitude"));
		LOG.info("Deserailiazed Record is: " + genericRecord);		
		
	}
	
    public  Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        
        return config;
    }		

	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
		// TODO Auto-generated method stub

	}

}
