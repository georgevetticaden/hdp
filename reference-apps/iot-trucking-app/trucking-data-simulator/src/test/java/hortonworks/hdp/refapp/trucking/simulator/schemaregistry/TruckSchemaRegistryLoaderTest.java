package hortonworks.hdp.refapp.trucking.simulator.schemaregistry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.junit.Before;
import org.junit.Test;

import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;

public class TruckSchemaRegistryLoaderTest {

	private static final String SCHEMA_REGISTRY_URL = "http://hdf-ref-app-web0.field.hortonworks.com:9090/api/v1";
	protected Logger LOG = Logger.getLogger(TruckSchemaRegistryLoader.class.getName());
	private TruckSchemaRegistryLoader registryLoader;
	
	
	@Before
	public void setup() {
		registryLoader = new TruckSchemaRegistryLoader(SCHEMA_REGISTRY_URL);
	}
	
	@Test
	public void loadSchemaRegistryWithTruckSchemas() {
		
		registryLoader.loadSchemaRegistry();
	}
	
	@Test
	public void getSchemaMetaDataForTruckGeoEvent() throws Exception {
		  
		SchemaMetadata schemaMetadata = registryLoader.createSchemaMetaForTruckGeoEvent();

	    SchemaMetadataInfo metaInfo = getSchemaMetaData(schemaMetadata);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema Metadata " + ReflectionToStringBuilder.toString(metaInfo));
	}
	

	@Test
	public void getSchemaMetaDataForTruckSpeedEvent() throws Exception {
		  
		SchemaMetadata schemaMetadata = registryLoader.createSchemaMetaForTruckSpeedEvent();
		
	    SchemaMetadataInfo metaInfo = getSchemaMetaData(schemaMetadata);
	    Assert.assertNotNull(metaInfo);
	    LOG.info("Schema MetaData is: " + ReflectionToStringBuilder.toString(metaInfo));
	}	
	
	
	@Test
	public void getSchemaForTruckGeoEvent() throws Exception {
		int version = 1;
		SchemaMetadata schemaMetadata = registryLoader.createSchemaMetaForTruckGeoEvent();
		SchemaVersionInfo schemaVersion = getSchemaByNameAndVersion(schemaMetadata.getName(), version);
		Assert.assertNotNull(schemaVersion);
		LOG.info("Schema for Truck Geo Event is: " + ReflectionToStringBuilder.toString(schemaVersion));
	}	
	
	
	
	@Test
	public void getSchemaForTruckSpeedEvent() throws Exception {
		int version = 1;
		SchemaMetadata schemaMetadata = registryLoader.createSchemaMetaForTruckSpeedEvent();
		SchemaVersionInfo schemaVersion = getSchemaByNameAndVersion(schemaMetadata.getName(), version);
		Assert.assertNotNull(schemaVersion);
		LOG.info(ReflectionToStringBuilder.toString(schemaVersion));
	}		
	
	
	@Test
	public void getLatestSchemaForTruckSpeedEvent() throws Exception {
		SchemaMetadata schemaMetadata = registryLoader.createSchemaMetaForTruckSpeedEvent();
		SchemaVersionInfo schemaVersion = getLatestSchema(schemaMetadata.getName());	
		Assert.assertNotNull(schemaVersion);
		LOG.info(ReflectionToStringBuilder.toString(schemaVersion));		
	}
	
	@Test
	public void getSeDeserializersForTruckGeoEvent() throws Exception {

		String schemaName = registryLoader.createSchemaMetaForTruckGeoEvent().getName();
		Collection<SerDesInfo> serdes = getSerializers(schemaName);
		Assert.assertNotNull(serdes);
		LOG.info("Number of Serdes is: " + serdes.size());
		for(SerDesInfo serde: serdes) {
			LOG.info(ReflectionToStringBuilder.toString(serde));
		
		}
	}	
	
	@Test
	public void getSeDeserializersForTruckSpeedEvent() throws Exception {

		String schemaName = registryLoader.createSchemaMetaForTruckSpeedEvent().getName();
		Collection<SerDesInfo> serdes = getSerializers(schemaName);
		Assert.assertNotNull(serdes);
		LOG.info("Number of Serdes is: " + serdes.size());
		for(SerDesInfo serde: serdes) {
			LOG.info(ReflectionToStringBuilder.toString(serde));
		}
	}		
	
	@Test
	public void serializeTruckGeoEvent() throws Exception  {
		
		
		SchemaMetadata schemaMetadata = registryLoader.createSchemaMetaForTruckGeoEvent();
		String schemaName = schemaMetadata.getName();
		
		//get serializer info from registry
		SerDesInfo serializerInfo = getSerializerFromRegistry(schemaName, TruckSchemaConfig.AVRO_SERIALIZER_NAME);
		Assert.assertNotNull("Couldnot find serializer["+TruckSchemaConfig.AVRO_SERIALIZER_NAME+"]", serializerInfo);
		
		//create and initialize serializer object
		SnapshotSerializer<Object, byte[], SchemaMetadata> avroSerializer = registryLoader.schemaRegistryClient.createSerializerInstance(serializerInfo);
		Assert.assertNotNull(avroSerializer);
		avroSerializer.init(registryLoader.createConfig(SCHEMA_REGISTRY_URL));
		
		
		//Load a avro data file that was created from CSV truck event using Kite Utilities
		InputStream inputStream = this.getClass().getResourceAsStream("/schema/samples/truck-geo-event-payload.avro");
		Assert.assertNotNull(inputStream);
		
		
		//convert the avro data file input stream into Generic Record using Kite
       GenericRecord avroGenericRecord = convertAvroDataFileToAVroGenericRecord(inputStream);	
       Assert.assertNotNull(avroGenericRecord);
       LOG.info("AVro Generic Record read from file before being serialized is: " + ReflectionToStringBuilder.toString(avroGenericRecord));
		

       // Now we have the payload in right format (Avro GenericRecord), lets serialize
		byte[] serializedPaylod = avroSerializer.serialize(avroGenericRecord, schemaMetadata);
		Assert.assertNotNull(serializedPaylod);

		FileUtils.writeByteArrayToFile(new File("truck-geo-event-payload.serialized"), serializedPaylod);
		
	}



	
	@Test
	public void deSerializeTruckGeoEvent() throws Exception {
		
		SchemaMetadata schemaMetadata = registryLoader.createSchemaMetaForTruckGeoEvent();
		String schemaName = schemaMetadata.getName();
		
		//get deserailizer from registry
		SerDesInfo deserializerInfo = getDeserializerFromRegistry(schemaName, TruckSchemaConfig.AVRO_DESERIALIZER_NAME);
		Assert.assertNotNull("Couldnot find serializer["+TruckSchemaConfig.AVRO_DESERIALIZER_NAME+"]", deserializerInfo);
		LOG.info("The derializer info is: " + ReflectionToStringBuilder.toString(deserializerInfo));
		
		//create and initialize deserializer object
		SnapshotDeserializer<InputStream, Object, SchemaMetadata, Integer> avroDeserializer  = registryLoader.schemaRegistryClient.createDeserializerInstance(deserializerInfo);
		Assert.assertNotNull(avroDeserializer);
		avroDeserializer.init(registryLoader.createConfig(SCHEMA_REGISTRY_URL));	
		
		//Load the serialized file 
		File file = new File("truck-geo-event-payload.serialized");
		Assert.assertTrue(file.exists());
		FileInputStream serializedTruckEventStream = new FileInputStream(file);
		Assert.assertNotNull(serializedTruckEventStream);
		
		Object avroTruckGeoEventRecord = avroDeserializer.deserialize(serializedTruckEventStream, schemaMetadata, null);
		Assert.assertNotNull(avroTruckGeoEventRecord);
		LOG.info(ReflectionToStringBuilder.toString(avroTruckGeoEventRecord));
		
		GenericRecord record = (GenericRecord)avroTruckGeoEventRecord;
		Object dataInRecord = record.get("data");
		LOG.info("data in record: " + ReflectionToStringBuilder.toString(dataInRecord));
		
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
		
		
	
	private SchemaMetadataInfo getSchemaMetaData(
			SchemaMetadata schemaMetadata) {
		SchemaMetadataInfo metaInfo= registryLoader.schemaRegistryClient.getSchemaMetadataInfo(schemaMetadata.getName());
		
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
	
	private SerDesInfo getSerializerFromRegistry(String schemaName, String serializerName) {
		Collection<SerDesInfo> serdes = getSerializers(schemaName);
		//Assert.assertEquals(2, serdes.size());
		
		SerDesInfo serializerInfo = null;
		Iterator<SerDesInfo> iter = serdes.iterator();
		while(iter.hasNext()) {
			SerDesInfo serDe = iter.next();
			if(serDe.getName().equals(serializerName)) {
				serializerInfo = serDe;
				break;
			}
		}
		return serializerInfo;
	}    
    
	private SerDesInfo getDeserializerFromRegistry(String schemaName, String deserializerName) {
		Collection<SerDesInfo> serdes = getDeserializers(schemaName);
		//Assert.assertEquals(2, serdes.size());
		
		SerDesInfo serializerInfo = null;
		Iterator<SerDesInfo> iter = serdes.iterator();
		while(iter.hasNext()) {
			SerDesInfo serDe = iter.next();
			if(serDe.getName().equals(deserializerName)) {
				serializerInfo = serDe;
				break;
			}
		}
		return serializerInfo;
	}  
	
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


		private Schema createAvroSchema(String schemaFileName) throws Exception{
	    	Schema schema = new Schema.Parser().parse(registryLoader.getSchema(schemaFileName)); 
	    	return schema;
	    }
	       
	  
	    private static class AvroUtil {

	        @SuppressWarnings("unchecked")
	        public static <D> DatumWriter<D> newDatumWriter(Schema schema, Class<D> dClass) {
	            return (DatumWriter<D>) GenericData.get().createDatumWriter(schema);
	        }

	        @SuppressWarnings("unchecked")
	        public static <D> DatumReader<D> newDatumReader(Schema schema, Class<D> dClass) {
	            return (DatumReader<D>) GenericData.get().createDatumReader(schema);
	        }

	    }	
		
	
}
