package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.junit.Before;
import org.junit.Test;

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.AvroSnapshotSerializer;


public class TruckSchemaRegistryLoaderTest {

	//private static final String SCHEMA_REGISTRY_URL = "http://fenton-hdf13.field.hortonworks.com:7788/api/v1";
	private static final String SCHEMA_REGISTRY_URL = "http://tp2-hdf1.field.hortonworks.com:7788/api/v1";
	protected Logger LOG = Logger.getLogger(TruckSchemaRegistryLoader.class.getName());
	private TruckSchemaRegistryLoader registryLoader;
	
	private static final String LINE_BREAK = "\n";
	private byte[] LINE_BREAK_BYTES = LINE_BREAK.getBytes();	
	
	
	@Before
	public void setup() {
		registryLoader = new TruckSchemaRegistryLoader(SCHEMA_REGISTRY_URL);
	}
	
	@Test
	public void loadSchemaRegistryWithTruckSchemas() {
		
		registryLoader.loadSchemaRegistry();
	}
	
	@Test
	public void getSchemaMetaDataForTruckGeoEventInLog() throws Exception {
		  

	    SchemaMetadataInfo metaInfo = getSchemaMetaData(TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_NAME);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema Metadata " + ReflectionToStringBuilder.toString(metaInfo));
	}
	

	@Test
	public void getSchemaMetaDataForTruckSpeedEventInLog() throws Exception {
		  
		SchemaMetadataInfo metaInfo = getSchemaMetaData(TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_NAME);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema MetaData is: " + ReflectionToStringBuilder.toString(metaInfo));
	}	
	
	@Test
	public void getSchemaMetaDataForRawTruckGeoEventInKafka() throws Exception {
		  

	    SchemaMetadataInfo metaInfo = getSchemaMetaData(TruckSchemaConfig.KAFKA_RAW_TRUCK_GEO_EVENT_SCHEMA_NAME);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema Metadata " + ReflectionToStringBuilder.toString(metaInfo));
	}	
	
	@Test
	public void getSchemaMetaDataForRawTruckSpeedEventInKafka() throws Exception {
		  
		SchemaMetadataInfo metaInfo = getSchemaMetaData(TruckSchemaConfig.KAFKA_RAW_TRUCK_SPEED_EVENT_SCHEMA_NAME);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema MetaData is: " + ReflectionToStringBuilder.toString(metaInfo));
	}	
	
	@Test
	public void getSchemaMetaDataForTruckGeoEventInKafka() throws Exception {

	    SchemaMetadataInfo metaInfo = getSchemaMetaData(TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_SCHEMA_NAME);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema Metadata " + ReflectionToStringBuilder.toString(metaInfo));
	}
	

	@Test
	public void getSchemaMetaDataForTruckSpeedEventInKafka() throws Exception {
		  
		SchemaMetadataInfo metaInfo = getSchemaMetaData(TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_SCHEMA_NAME);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema MetaData is: " + ReflectionToStringBuilder.toString(metaInfo));
	}	
	
	
	@Test
	public void getSchemaForTruckGeoEventInLog() throws Exception {
		SchemaVersionInfo schemaVersion = getSchemaByNameAndVersion(TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_SCHEMA_NAME, 
																   TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_SCHEMA_VERSION);
		Assert.assertNotNull(schemaVersion);
		LOG.info("Schema for Truck Geo Event is: " + ReflectionToStringBuilder.toString(schemaVersion));
	}	
	
	@Test
	public void getSchemaForTruckGeoEventInLogNifiVersion() throws Exception {

	    SchemaMetadataInfo metaInfo = getSchemaMetaData(TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_NAME);
	    long schemaId = metaInfo.getId();
	    SchemaMetadataInfo metaInfoById =  getSchemaMetaData(schemaId);
	    
	    
	    SchemaMetadata metadata = metaInfoById.getSchemaMetadata();
        final String schemaName = metadata.getName();	    
		
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_VERSION);
        final SchemaVersionInfo versionInfo = registryLoader.schemaRegistryClient.getSchemaVersionInfo(schemaVersionKey);    
        
       
		LOG.info("Schema for Truck Geo Event is: " + ReflectionToStringBuilder.toString(versionInfo));
	}		
	
	
	@Test
	public void getSchemaForTruckSpeedEventInLog() throws Exception {

		SchemaVersionInfo schemaVersion = getSchemaByNameAndVersion(TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_NAME, 
																	TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_VERSION);
		Assert.assertNotNull(schemaVersion);
		LOG.info(ReflectionToStringBuilder.toString(schemaVersion));
	}		
	
	@Test
	public void getSchemaForTruckGeoEventInKafka() throws Exception {
		SchemaVersionInfo schemaVersion = getSchemaByNameAndVersion(TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_SCHEMA_NAME, 
																   TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_SCHEMA_VERSION);
		Assert.assertNotNull(schemaVersion);
		LOG.info("Schema for Truck Geo Event is: " + ReflectionToStringBuilder.toString(schemaVersion));
	}	
	
	
	
	@Test
	public void getSchemaForTruckSpeedEventInKafka() throws Exception {

		SchemaVersionInfo schemaVersion = getSchemaByNameAndVersion(TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_SCHEMA_NAME, 
																	TruckSchemaConfig.KAFKA_TRUCK_SPEED_EVENT_SCHEMA_VERSION);
		Assert.assertNotNull(schemaVersion);
		LOG.info(ReflectionToStringBuilder.toString(schemaVersion));
	}		
	
	
	@Test
	public void getLatestSchemaForTruckSpeedEventInLog() throws Exception {
		SchemaVersionInfo schemaVersion = getLatestSchema(TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_NAME);	
		Assert.assertNotNull(schemaVersion);
		LOG.info(ReflectionToStringBuilder.toString(schemaVersion));		
	}
	
	@Test
	public void getSeDeserializersForTruckGeoEventInLog() throws Exception {

		Collection<SerDesInfo> serdes = getSerializers(TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_NAME);
		Assert.assertNotNull(serdes);
		LOG.info("Number of Serdes is: " + serdes.size());
		for(SerDesInfo serde: serdes) {
			LOG.info(ReflectionToStringBuilder.toString(serde));
		
		}
	}	
	
	@Test
	public void getSeDeserializersForTruckSpeedEventInLog() throws Exception {

		Collection<SerDesInfo> serdes = getSerializers(TruckSchemaConfig.LOG_TRUCK_SPEED_EVENT_SCHEMA_NAME);
		Assert.assertNotNull(serdes);
		LOG.info("Number of Serdes is: " + serdes.size());
		for(SerDesInfo serde: serdes) {
			LOG.info(ReflectionToStringBuilder.toString(serde));
		}
	}		
	
	@Test
	public void serializeTruckGeoEvent() throws Exception  {

		
		//get serializer info from registry
		AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();		
		serializer.init(TruckSchemaRegistryLoader.createConfig(SCHEMA_REGISTRY_URL));
		Assert.assertNotNull(serializer);		
		
		
		//Object deviceObject = createGenericRecordForDevice("/device.avsc");
		
		//Load a avro data file that was created from CSV truck event using Kite Utilities
		InputStream inputStream = this.getClass().getResourceAsStream("/schema/samples/truck-geo-event-payload.avro");
		Assert.assertNotNull(inputStream);
		
		
		//convert the avro data file input stream into Generic Record using Kite
       GenericRecord avroGenericRecord = convertAvroDataFileToAVroGenericRecord(inputStream);	
       Assert.assertNotNull(avroGenericRecord);
       LOG.info("AVro Generic Record read from file before being serialized is: " + ReflectionToStringBuilder.toString(avroGenericRecord));
	
       // Now we have the payload in right format (Avro GenericRecord), lets serialize
       SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_NAME)
		  .type(AvroSchemaProvider.TYPE)
		  .schemaGroup(TruckSchemaConfig.LOG_SCHEMA_GROUP_NAME)
		  .description("Speed Events from trucks")
		  .compatibility(SchemaCompatibility.BACKWARD)
		  .build();       
		byte[] serializedPaylod = serializer.serialize(avroGenericRecord, schemaMetadata);
		Assert.assertNotNull(serializedPaylod);

		FileUtils.writeByteArrayToFile(new File("truck-geo-event-payload.serialized"), serializedPaylod);
		
	}
	
	
    protected Object createGenericRecordForTruckGeoEvent(String schemaFileName) throws IOException {
        Schema schema = new Schema.Parser().parse(getSchema(schemaFileName));

        
        GenericRecord avroRecord = new GenericData.Record(schema);
        String eventTime = new Timestamp(new Date().getTime()).toString();
        avroRecord.put("eventTime", eventTime);
        avroRecord.put("eventSource", "truck_geo_event");
        avroRecord.put("truckId", 40);
        avroRecord.put("driverId", 23);
        avroRecord.put("driverName", "Jeff Markham");
        avroRecord.put("routeId", 1345);
        avroRecord.put("route", "Saint Louis to Chicago");
        avroRecord.put("eventType", "Lane Departure");
        avroRecord.put("latitude", -82.52);
        avroRecord.put("longitude", 40.7);
        long correlationId = 100;
        avroRecord.put("correlationId", correlationId);
        

        return avroRecord;
    }	
    
    private String getSchema(String schemaFileName) throws IOException {
        InputStream schemaResourceStream = this.getClass().getResourceAsStream(schemaFileName);
        if (schemaResourceStream == null) {
            throw new IllegalArgumentException("Given schema file [" + schemaFileName + "] does not exist");
        }

        String schemaText = IOUtils.toString(schemaResourceStream, "UTF-8");
        LOG.info("Schema Text is" + schemaText);
        return schemaText;
    }    
	
	@Test
	public void serializeTruckGeoEventWithGenericOject() throws Exception  {
		
		
		
		//get serializer info from registry
		AvroSnapshotSerializer serializer = new AvroSnapshotSerializer();		
		serializer.init(TruckSchemaRegistryLoader.createConfig(SCHEMA_REGISTRY_URL));
		Assert.assertNotNull(serializer);		
		
		
		Object truckGeoEvent = createGenericRecordForTruckGeoEvent("/schema/truck-geo-event-kafka.avsc");

       LOG.info("AVro Generic Record read  created is: " + ReflectionToStringBuilder.toString(truckGeoEvent));
	
       // Now we have the payload in right format (Avro GenericRecord), lets serialize
       SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(TruckSchemaConfig.KAFKA_TRUCK_GEO_EVENT_SCHEMA_NAME)
		  .type(AvroSchemaProvider.TYPE)
		  .schemaGroup(TruckSchemaConfig.KAFKA_SCHEMA_GROUP_NAME)
		  .description("Truck Geo Events")
		  .compatibility(SchemaCompatibility.BACKWARD)
		  .build();       
		byte[] serializedPaylod = serializer.serialize(truckGeoEvent, schemaMetadata);
		Assert.assertNotNull(serializedPaylod);

		FileUtils.writeByteArrayToFile(new File("truck-geo-event-payload.serialized"), serializedPaylod);
		
	}	


	
	@Test
	public void deSerializeTruckGeoEvent() throws Exception {
		
		
		//get deserailizer from registry
		//create and initialize derserializer object	
	    AvroSnapshotDeserializer deserializer = registryLoader.schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE);
	    deserializer.init(TruckSchemaRegistryLoader.createConfig(SCHEMA_REGISTRY_URL));
	    Assert.assertNotNull(deserializer);
	    	
		
		//Load the serialized file 
		
		InputStream serializedTruckEventStream = this.getClass().getResourceAsStream("/schema/samples/truck-geo-event-payload.serialized");

		Assert.assertNotNull(serializedTruckEventStream);
		
		//deserialize
	       SchemaMetadata schemaMetadata = new SchemaMetadata.Builder(TruckSchemaConfig.LOG_TRUCK_GEO_EVENT_SCHEMA_NAME)
			  .type(AvroSchemaProvider.TYPE)
			  .schemaGroup(TruckSchemaConfig.LOG_SCHEMA_GROUP_NAME)
			  .description("Speed Events from trucks")
			  .compatibility(SchemaCompatibility.BACKWARD)
			  .build(); 		
		Object avroTruckGeoEventRecord = deserializer.deserialize(serializedTruckEventStream, null);
		Assert.assertNotNull(avroTruckGeoEventRecord);
		
		GenericRecord record = (GenericRecord)avroTruckGeoEventRecord;
		LOG.info("I dont' trust toString, Longtitudde is: " + record.get("longitude"));
		LOG.info("Record is: " + record);
		
	}
	
//	  	@Test
//	    public void convertCSVtoAvro() throws Exception{
//
//			String schemaFileName = "/schema/truck-geo-event.avsc";
//	        final Schema schema = createAvroSchema(schemaFileName);
//	        Assert.assertNotNull(schema);
//	        
//	        String csvPayLoadFile = "/schema/samples/truck-geo-event-payload.csv";
//	        InputStream truckGeoEventCSVInputStream = TruckSchemaRegistryLoaderTest.class.getResourceAsStream(csvPayLoadFile);
//	        Assert.assertNotNull(truckGeoEventCSVInputStream);
//
//	        
//	        //private CSVProperties(String charset, String delimiter, String quote,String escape, String header, boolean useHeader,int linesToSkip
//	        
//	        CSVProperties csvProperties = createCSVProperties();
//	        
//	        final DataFileWriter<Record> writer = new DataFileWriter<>(AvroUtil.newDatumWriter(schema, Record.class));
//	        writer.setCodec(CodecFactory.snappyCodec());
//	        
//			CSVFileReader<Record> reader = new CSVFileReader<>(truckGeoEventCSVInputStream, csvProperties, schema, Record.class);
//	        reader.initialize();        
//	        
//	        File file = new File("truck-geo-event-payload.avro");
//	        OutputStream avroOutPutStream = new FileOutputStream(file);
//	        Assert.assertNotNull(avroOutPutStream);
//	        DataFileWriter<Record> w = writer.create(schema, avroOutPutStream);
//	        
//	        while (reader.hasNext()) {
//	            try {
//	                Record record = reader.next();
//	                w.append(record);
//	            } catch (DatasetRecordException e) {
//	                throw e;
//	            }
//	        }
//	        writer.close();
//	        avroOutPutStream.close();
//	               
//	                
//	    }	


	private GenericRecord convertAvroDataFileToAVroGenericRecord(
			InputStream inputStream) throws IOException {
		GenericRecord currRecord = null;
		   DataFileStream<GenericRecord> reader = new DataFileStream<>(inputStream, new GenericDatumReader<GenericRecord>());
		   if (reader.hasNext()) {
		       currRecord = reader.next();
		   }
		return currRecord;
	}
		
		
	
	private SchemaMetadataInfo getSchemaMetaData(String schemaName) {
		SchemaMetadataInfo metaInfo= registryLoader.schemaRegistryClient.getSchemaMetadataInfo(schemaName);
		
		return metaInfo;
	}
	
	private SchemaMetadataInfo getSchemaMetaData(long schemaId) {
		SchemaMetadataInfo metaInfo= registryLoader.schemaRegistryClient.getSchemaMetadataInfo(schemaId);
		
		return metaInfo;
	}	
	
	private SchemaVersionInfo getSchemaByNameAndVersion(String schemaName, int version)
			throws SchemaNotFoundException {
		SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, version);
		SchemaVersionInfo schemaVersion = registryLoader.schemaRegistryClient.getSchemaVersionInfo(schemaVersionKey);
		return schemaVersion;
	}	
	
	private SchemaVersionInfo getLatestSchema(String schemaName)
			throws SchemaNotFoundException {
		SchemaVersionInfo schemaVersion  = registryLoader.schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
		return schemaVersion;
	}	
	
	

	
	private Collection<SerDesInfo> getSerializers(String schemaName) {
		Collection<SerDesInfo> serdes = registryLoader.schemaRegistryClient.getSerializers(schemaName);
		return serdes;
	}

	
	private Collection<SerDesInfo> getDeserializers(String schemaName) {
		Collection<SerDesInfo> serdes = registryLoader.schemaRegistryClient.getDeserializers(schemaName);
		return serdes;
	}	
	
//	private SerDesInfo getSerializerFromRegistry(String schemaName, String serializerName) {
//		Collection<SerDesInfo> serdes = getSerializers(schemaName);
//		//Assert.assertEquals(2, serdes.size());
//		
//		SerDesInfo serializerInfo = null;
//		Iterator<SerDesInfo> iter = serdes.iterator();
//		while(iter.hasNext()) {
//			SerDesInfo serDe = iter.next();
//			if(serDe.getName().equals(serializerName)) {
//				serializerInfo = serDe;
//				break;
//			}
//		}
//		return serializerInfo;
//	}    
    
//	private SerDesInfo getDeserializerFromRegistry(String schemaName, String deserializerName) {
//		Collection<SerDesInfo> serdes = getDeserializers(schemaName);
//		//Assert.assertEquals(2, serdes.size());
//		
//		SerDesInfo serializerInfo = null;
//		Iterator<SerDesInfo> iter = serdes.iterator();
//		while(iter.hasNext()) {
//			SerDesInfo serDe = iter.next();
//			if(serDe.getName().equals(deserializerName)) {
//				serializerInfo = serDe;
//				break;
//			}
//		}
//		return serializerInfo;
//	}  
	
//	 private CSVProperties createCSVProperties() {
//	    	
//	    	CSVProperties DEFAULTS = new CSVProperties.Builder().build();
//	    	
//	    	return new CSVProperties.Builder()
//	        .charset(DEFAULTS.charset)
//	        .delimiter("|")
//	        .quote(DEFAULTS.quote)
//	        .escape(DEFAULTS.escape)
//	        .hasHeader(DEFAULTS.useHeader)
//	        .linesToSkip(DEFAULTS.linesToSkip)
//	        .build();
//		}


//		private Schema createAvroSchema(String schemaFileName) throws Exception{
//	    	Schema schema = new Schema.Parser().parse(registryLoader.getSchema(schemaFileName)); 
//	    	return schema;
//	    }
//	       
//	  
//	    private static class AvroUtil {
//
//	        @SuppressWarnings("unchecked")
//	        public static <D> DatumWriter<D> newDatumWriter(Schema schema, Class<D> dClass) {
//	            return (DatumWriter<D>) GenericData.get().createDatumWriter(schema);
//	        }
//
//	        @SuppressWarnings("unchecked")
//	        public static <D> DatumReader<D> newDatumReader(Schema schema, Class<D> dClass) {
//	            return (DatumReader<D>) GenericData.get().createDatumReader(schema);
//	        }
//
//	    }	
//		
	
}
