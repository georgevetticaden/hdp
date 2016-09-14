package hortonworks.hdp.refapp.trucking.storm.kafka;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class TruckEventSchema implements Scheme{

	private static final long serialVersionUID = -2990121166902741545L;

	private static final Logger LOG = LoggerFactory.getLogger(TruckEventSchema.class);
	

	@Override
	public List<Object> deserialize(ByteBuffer buffer) {
		try {
			String[] pieces = convertRawEvent(buffer.array());
			
			Timestamp eventTime = Timestamp.valueOf(pieces[0]);
			int truckId = Integer.valueOf(pieces[1]);
			int driverId = Integer.valueOf(pieces[2]);
			String driverName = pieces[3];
			int routeId = Integer.valueOf(pieces[4]);
			String routeName = pieces[5];
			String eventType = pieces[6];
			double latitude= Double.valueOf(pieces[7]);
			double longitude  = Double.valueOf(pieces[8]);	
			long correlationId = Long.valueOf(pieces[9]);
			String eventKey = consructKey(driverId, truckId, eventTime);
			
			if(LOG.isTraceEnabled()) {
				LOG.trace("Creating a Truck Scheme with driverId["+driverId + "], driverName["+driverName+"], routeId["+routeId+"], routeName["+ routeName +"], "
						+ "eventType["+eventType+"], latitude["+latitude + "], longitude["+longitude+"], eventKey["+eventKey + "], and correlationId["+correlationId +"]");				
			}

			
			return new Values(driverId, truckId, eventTime, eventType, longitude, latitude, eventKey, correlationId, driverName, routeId, routeName);
			
		} catch (Exception e) {
			LOG.error("Error serializeing truck event in Kafka Spout",  e);
			return null;
		}
		
	}


	private String[] convertRawEvent(byte[] bytes) throws Exception {
		
		String truckEvent = new String(bytes, "UTF-8");
		if(LOG.isTraceEnabled()) {
			LOG.trace("Raw Truck Event is: " + truckEvent);
		}
		
		String initialPieces[] = truckEvent.split("DIVIDER") ;
		String[] pieces = initialPieces[1].split("\\|");
		return pieces;
	}



	@Override
	public Fields getOutputFields() {
		return new Fields("driverId", "truckId", "eventTime", "eventType", "longitude", "latitude", "eventKey", "correlationId", "driverName", "routeId", "routeName");
		
	}
	
	private String consructKey(int driverId, int truckId, Timestamp ts2) {
		long reverseTime = Long.MAX_VALUE - ts2.getTime();
		String rowKey = driverId+"|"+truckId+"|"+reverseTime;
		return rowKey;
	}



}