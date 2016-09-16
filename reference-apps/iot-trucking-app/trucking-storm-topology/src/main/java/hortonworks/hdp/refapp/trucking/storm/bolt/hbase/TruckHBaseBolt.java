package hortonworks.hdp.refapp.trucking.storm.bolt.hbase;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class TruckHBaseBolt implements IRichBolt {


	private static final byte[] INCIDENT_RUNNING_TOTAL_COLUMN = Bytes.toBytes("incidentRunningTotal");
	private static final long serialVersionUID = 2946379346389650318L;
	private static final Logger LOG = LoggerFactory.getLogger(TruckHBaseBolt.class);
	
	private static final String DANGEROUS_EVENTS_TABLE_NAME = "driver_dangerous_events";
	private static final String EVENTS_TABLE_COLUMN_FAMILY_NAME = "events";	
	
	
	private static final String EVENTS_TABLE_NAME = "driver_events";
	private static final String ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME = "allevents";	
	
	private static final String EVENTS_COUNT_TABLE_NAME = "driver_dangerous_events_count";
	private static final String EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME = "counters";	

	
	private OutputCollector collector;
	private HConnection connection;
	private HTableInterface dangerousEventsTable;
	private HTableInterface eventsCountTable;
	private HTableInterface eventsTable;
	
	private boolean persistAllEvents;
	
	private Properties config;

	public TruckHBaseBolt(Properties topologyConfig) {
		this.config = topologyConfig;
		this.persistAllEvents = Boolean.valueOf(config.getProperty("trucking.hbase.persist.all.events")).booleanValue();
		LOG.info("The PersistAllEvents Flag is set to: " + persistAllEvents);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		try {
			this.connection = HConnectionManager.createConnection(constructConfiguration());
			this.dangerousEventsTable = connection.getTable(DANGEROUS_EVENTS_TABLE_NAME);
			this.eventsCountTable = connection.getTable(EVENTS_COUNT_TABLE_NAME);	
			this.eventsTable = connection.getTable(EVENTS_TABLE_NAME);
			
		} catch (Exception e) {
			String errMsg = "Error retrievinging connection and access to dangerousEventsTable";
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}		
	}

	@Override
	public void execute(Tuple input) {
		
		LOG.info("About to insert tuple["+input +"] into HBase...");
		
		
		
		int driverId = input.getIntegerByField("driverId");
		int truckId = input.getIntegerByField("truckId");
		Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
		String eventType = input.getStringByField("eventType");
		double longitude = input.getDoubleByField("longitude");
		double latitude = input.getDoubleByField("latitude");
		String driverName = input.getStringByField("driverName");
		int routeId = input.getIntegerByField("routeId");
		String routeName = input.getStringByField("routeName");
		
		long incidentTotalCount = getInfractionCountForDriver(driverId);
		
		if(!eventType.equals("Normal")) {
			try {
				
				//Store the incident event in HBase
				Put put = constructRow(EVENTS_TABLE_COLUMN_FAMILY_NAME, driverId, truckId, eventTime, eventType,
						latitude, longitude, driverName, routeId, routeName);
				this.dangerousEventsTable.put(put);
				LOG.info("Success inserting event into HBase table["+DANGEROUS_EVENTS_TABLE_NAME+"]");
				
				//Update the running count of all incidents
				incidentTotalCount = this.eventsCountTable.incrementColumnValue(Bytes.toBytes(driverId), Bytes.toBytes(EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME), 
															INCIDENT_RUNNING_TOTAL_COLUMN, 1L);
				LOG.info("Success inserting event into counts table");

			} catch (Exception e) {
				LOG.error("	Error inserting violation event into HBase table", e);
			}				
		} 
		
		/* If persisting all events, then store into the driver_events table */
		if(persistAllEvents) {

			//Store the  event in HBase
			try {
				
				Put put = constructRow(ALL_EVENTS_TABLE_COLUMN_FAMILY_NAME, driverId, truckId, eventTime, eventType,
						latitude, longitude, driverName, routeId, routeName);
				this.eventsTable.put(put);
				LOG.info("Success inserting event into HBase table["+EVENTS_TABLE_NAME+"]");				
			} catch (Exception e) {
				LOG.error("	Error inserting event into HBase table["+EVENTS_TABLE_NAME+"]", e);
			}

		}
		
		collector.emit(input, new Values(driverId, truckId, eventTime, eventType, longitude, latitude, incidentTotalCount, driverName, routeId, routeName));
		
		//acknowledge even if there is an error
		collector.ack(input);
		
		
	}
	
	
	
	/**
	 * @param zookeeperHost 
	 * @param zookeeperClientPort 
	 * @param zookeeperZNodeParent 
	 * @return
	 */
	public  Configuration constructConfiguration() {
		
		String zookeeperHost = this.config.getProperty("hbase.zookeeper.host");
		String zookeeperClientPort = this.config.getProperty("hbase.zookeeper.client.port");
		String zookeeperZNodeParent = this.config.getProperty("hbase.zookeeper.znode.parent");
		
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				zookeeperHost);
		config.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
		config.set("zookeeper.znode.parent", zookeeperZNodeParent);
		return config;
	}	

	
	private Put constructRow(String columnFamily, int driverId, int truckId, Timestamp eventTime, String eventType, double latitude, double longitude, String driverName, int routeId, String routeName ) {
		
		String rowKey = consructKey(driverId, truckId, eventTime);
		System.out.println("Record with key["+rowKey + "] going to be inserted...");
		Put put = new Put(Bytes.toBytes(rowKey));
		
		String driverColumn = "driverId";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(driverColumn), Bytes.toBytes(driverId));
		
		String truckColumn = "truckId";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(truckColumn), Bytes.toBytes(truckId));
		
		String eventTimeColumn = "eventTime";
		long eventTimeValue=  eventTime.getTime();
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(eventTimeColumn), Bytes.toBytes(eventTimeValue));
		
		String eventTypeColumn = "eventType";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(eventTypeColumn), Bytes.toBytes(eventType));
		
		String latColumn = "latitudeColumn";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(latColumn), Bytes.toBytes(latitude));
		
		String longColumn = "longitudeColumn";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(longColumn), Bytes.toBytes(longitude));
		
		String driverNameColumn = "driverName";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(driverNameColumn), Bytes.toBytes(driverName));
		
		String routeIdColumn = "routeId";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(routeIdColumn), Bytes.toBytes(routeId));	

		String routeNameColumn = "routeName";
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(routeNameColumn), Bytes.toBytes(routeName));			
		return put;
	}


	private String consructKey(int driverId, int truckId, Timestamp ts2) {
		long reverseTime = Long.MAX_VALUE - ts2.getTime();
		String rowKey = driverId+"|"+truckId+"|"+reverseTime;
		return rowKey;
	}	
	
	
	@Override
	public void cleanup() {
		try {
			dangerousEventsTable.close();
			eventsCountTable.close();
			eventsTable.close();
			connection.close();
		} catch (Exception  e) {
			LOG.error("Error closing connections", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("driverId", "truckId", "eventTime", "eventType", "longitude", "latitude", "incidentTotalCount", "driverName", "routeId", "routeName"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	private long getInfractionCountForDriver(int driverId) {
		try {
			byte[] driverCount = Bytes.toBytes(driverId);
			Get get = new Get(driverCount);
			Result result = eventsCountTable.get(get);
			long count = 0;
			if(result != null) {
				byte[] countBytes = result.getValue(Bytes.toBytes(EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME), INCIDENT_RUNNING_TOTAL_COLUMN);
				if(countBytes != null) 
				{
					count = Bytes.toLong(countBytes);
				}
				
			}
			return count;
		} catch (Exception e) {
			LOG.error("Error getting infraction count", e);
			throw new RuntimeException("Error getting infraction count");
		}
	}	
}
