package hortonworks.hdp.refapp.trucking.storm.bolt.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

public class TruckPhoenixHBaseBolt implements IRichBolt {


	private static final byte[] INCIDENT_RUNNING_TOTAL_COLUMN = Bytes.toBytes("incidentRunningTotal");
	private static final long serialVersionUID = 2946379346389650318L;
	private static final Logger LOG = LoggerFactory.getLogger(TruckPhoenixHBaseBolt.class);
	private static final String EVENTS_COUNT_TABLE_NAME = "driver_dangerous_events_count";
	private static final String EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME = "counters";	

	
	private OutputCollector collector;
	private HConnection hbaseConnection;
	private HTableInterface eventsCountTable;
	
	private boolean persistAllEvents;
	private Properties config;
	
	private String phoenixConnectionUrl;
	private Connection phoenixConnection;

	public TruckPhoenixHBaseBolt(Properties topologyConfig) {
		this.config = topologyConfig;
		this.persistAllEvents = Boolean.valueOf(topologyConfig.getProperty("trucking.hbase.persist.all.events")).booleanValue();
		this.phoenixConnectionUrl = String.valueOf(topologyConfig.getProperty("phoenix.connection.url"));
		LOG.info("The PersistAllEvents Flag is set to: " + persistAllEvents);
		LOG.info("Phoenix Connection Url is: " + phoenixConnectionUrl);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		try {
			this.hbaseConnection = HConnectionManager.createConnection(constructConfiguration());
			this.eventsCountTable = hbaseConnection.getTable(EVENTS_COUNT_TABLE_NAME);	
			
			this.phoenixConnection = DriverManager.getConnection(phoenixConnectionUrl);
			this.phoenixConnection.setAutoCommit(true);
						
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
		
		String rowKey = consructKey(driverId, truckId, eventTime);
		long incidentTotalCount = getInfractionCountForDriver(driverId);
		
		if(!eventType.equals("Normal")) {
			PreparedStatement updateStatement = null;
			try {
				
				//Store the violation event in HBase via Phoenix
				updateStatement = phoenixConnection.prepareStatement("upsert into truck.dangerous_events(id, driver_id, truck_id, event_time, latitude, longitude, event_type, driver_name, route_id, route_name) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				setParameters(updateStatement, rowKey, truckId, driverId, eventTime, latitude, longitude, eventType, driverName, routeId, routeName);
				updateStatement.executeUpdate();
				
				LOG.info("Success inserting event into Phoenix HBase table[truck.dangerous_events]");
				
				//Update the running count of all incidents
				incidentTotalCount = this.eventsCountTable.incrementColumnValue(Bytes.toBytes(driverId), Bytes.toBytes(EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME), 
															INCIDENT_RUNNING_TOTAL_COLUMN, 1L);
				LOG.info("Success inserting event into counts table");

			} catch (Exception e) {
				LOG.error("	Error inserting violation event into Phoenix HBase table", e);
			} finally {
				if(updateStatement != null) {
					try {
						updateStatement.close();
					} catch (SQLException e) {
						LOG.error("Error doing update", e);
					}
				}
			}
		} 
		
		/* If persisting all events, then store into the driver_events table */
		if(persistAllEvents) {
			
			PreparedStatement updateStatement = null;
			//Store the  event in HBase
			try {
				
				//Store the violation event in HBase via Phoenix
				updateStatement = phoenixConnection.prepareStatement("upsert into truck.events(id, driver_id, truck_id, event_time, latitude, longitude, event_type, driver_name, route_id, route_name) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				setParameters(updateStatement, rowKey, truckId, driverId, eventTime, latitude, longitude, eventType, driverName, routeId, routeName);
				updateStatement.executeUpdate();
				
				LOG.info("Success inserting event into Phoenix HBase table[truck.events]");

							
			} catch (Exception e) {
				LOG.error("	Error inserting event into Phoenix HBase table[truck.events]", e);
			} finally {
				if(updateStatement != null) {
					try {
						updateStatement.close();
					} catch (SQLException e) {
						LOG.error("Error doing update", e);
					}
				}
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


	private String consructKey(int driverId, int truckId, Timestamp ts2) {
		long reverseTime = Long.MAX_VALUE - ts2.getTime();
		String rowKey = driverId+"|"+truckId+"|"+reverseTime;
		return rowKey;
	}	
	
	
	@Override
	public void cleanup() {
		try {
			eventsCountTable.close();
			phoenixConnection.close();
			hbaseConnection.close();
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
	
	private void setParameters(PreparedStatement updateStatement, String id, int truckId,  int driverId, Timestamp eventTime, double latitude, double longitude, String eventType, String driverName, int routeId, String routeName)
			throws SQLException {
		updateStatement.setString(1, id);
		updateStatement.setInt(2, driverId);
		updateStatement.setInt(3, truckId);
		updateStatement.setTimestamp(4, eventTime);
		updateStatement.setDouble(5, latitude);
		updateStatement.setDouble(6, longitude);
		updateStatement.setString(7,  eventType);
		updateStatement.setString(8, driverName);
		updateStatement.setInt(9,  routeId);
		updateStatement.setString(10, routeName);
	}	

}
