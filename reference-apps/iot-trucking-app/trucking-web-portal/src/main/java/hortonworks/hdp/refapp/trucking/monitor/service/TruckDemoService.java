package hortonworks.hdp.refapp.trucking.monitor.service;

import hortonworks.hdp.apputil.hbase.HBaseUtils;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.trucking.domain.TruckDriverViolationEvent;
import hortonworks.hdp.refapp.trucking.monitor.DemoResetParam;
import hortonworks.hdp.refapp.trucking.monitor.StreamGeneratorParam;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.TruckConfiguration;
import hortonworks.hdp.refapp.trucking.simulator.impl.messages.StartSimulation;
import hortonworks.hdp.refapp.trucking.simulator.listeners.SimulatorListener;
import hortonworks.hdp.refapp.trucking.simulator.masters.SimulationMaster;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@Service
public class TruckDemoService {
	
	private static final String DRIVER_EVENTS_TABLE = "driver_dangerous_events";
	private static String DRIVER_EVENTS_COLUMN_FAMILY_NAME = "events";
	private static final String DRIVER_EVENTS_COUNT_TABLE = "driver_dangerous_events_count";
	private static String DRIVER_EVENTS_COUNT_COLUMN_FAMILY_NAME = "counters";	
	
	private static final Logger LOG = LoggerFactory.getLogger(TruckDemoService.class);
	
	//Default Coordinates for Saint Louis
	public static final double STL_LAT= 38.523884;
	public static final double STL_LONG= -92.159845;
	public static final int DEFAULT_ZOOME_LEVEL = 6;
	public static final int DEFAULT_TRUCK_SYMBOL_SIZE = 10000;
	
	
	private HDPServiceRegistry registry;
	
	

	@Autowired
	public void TruckDemoService(HDPServiceRegistry registry) {
		this.registry = registry;
	}
	

	public void createTablesForTruckingApp() throws Exception {
		HBaseUtils hbaseUtils = new HBaseUtils(registry);
		hbaseUtils.createHBaseTable(DRIVER_EVENTS_TABLE, DRIVER_EVENTS_COLUMN_FAMILY_NAME);
		hbaseUtils.createHBaseTable(DRIVER_EVENTS_COUNT_TABLE, DRIVER_EVENTS_COUNT_COLUMN_FAMILY_NAME);
		hbaseUtils.createHBaseTable("driver_events", "allevents");
	}

	/**
	 * Returns all the driver events for all drivers
	 * @param registry 
	 * @return
	 */
	public Collection<TruckDriverViolationEvent> getLatestEventsForAllDrivers() {
		HConnection connection = null;
		HTableInterface driverEventsTable = null;
		HTableInterface driverEventsCountTable = null;
		
		try {
			Configuration config = constructConfiguration();
			connection = HConnectionManager.createConnection(config);
			driverEventsTable = connection.getTable(DRIVER_EVENTS_TABLE);
			driverEventsCountTable = connection.getTable(DRIVER_EVENTS_COUNT_TABLE);
			
			ResultScanner resultScanner = driverEventsTable.getScanner(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME));
			Map<Integer, TruckDriverViolationEvent> eventsMap = new HashMap<Integer, TruckDriverViolationEvent> ();
			for(Result result = resultScanner.next(); result != null ; result = resultScanner.next()) {
				
				//String key = Bytes.toString(result.getRow());
				int driverId = Bytes.toInt(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("driverId")));
				if(eventsMap.get(driverId) == null) {
					int truckId = Bytes.toInt(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("truckId")));
					String truckDriverEventKey = driverId + "|" + truckId;
					
					long eventTimeLong = Bytes.toLong(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("eventTime")));
					SimpleDateFormat sdf = new SimpleDateFormat();
					String timeStampString = sdf.format(eventTimeLong);	
					
					double longitude = Bytes.toDouble(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("longitudeColumn")));
					double latitude = Bytes.toDouble(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("longitudeColumn")));
					
					String lastInfraction = Bytes.toString(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("eventType")));
					long numberOfInfractions = getInfractionCountForDriver(driverEventsCountTable, driverId);
					
					int routeId = Bytes.toInt(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("routeId")));
					String driverName = Bytes.toString(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("driverName")));
		
					String routeName = Bytes.toString(result.getValue(Bytes.toBytes(DRIVER_EVENTS_COLUMN_FAMILY_NAME), Bytes.toBytes("routeName")));
					
					TruckDriverViolationEvent event = new TruckDriverViolationEvent(truckDriverEventKey, driverId, truckId, eventTimeLong, 
										timeStampString, longitude, latitude, lastInfraction, numberOfInfractions, driverName, routeId, routeName );
					
					eventsMap.put(driverId, event);
				}
			}
			return eventsMap.values();
		} catch (Exception e) {
			LOG.error("Error getting driver events", e);
			throw new RuntimeException("Error getting driver events", e);
		} finally {
			try {
				driverEventsTable.close();
				driverEventsCountTable.close();
				connection.close();
			} catch (IOException e) {
					//swallow
			}
		}
	}
	
	

	/**
	 * Returns all the driver events for all Drivers
	 * @param hdpServiceRegistry 
	 * @return
	 */
	public Collection<TruckDriverViolationEvent> getLatestEventsForAllDriversPhoenix() {
		
		Connection phoenixConnection = null;
		PreparedStatement statement = null;
		
		HTableInterface driverEventsCountTable = null;
		HConnection connection = null;
		try {
			
			phoenixConnection = DriverManager.getConnection(registry.getPhoenixConnectionURL());
			
			Configuration config = constructConfiguration();
			connection = HConnectionManager.createConnection(config);
			driverEventsCountTable = connection.getTable(DRIVER_EVENTS_COUNT_TABLE);			
			
			statement = phoenixConnection.prepareStatement("select * from  truck.dangerous_events order by driver_id asc, event_time desc");
			ResultSet resultSet = statement.executeQuery();
			Map<Integer, TruckDriverViolationEvent> eventsMap = new HashMap<Integer, TruckDriverViolationEvent> ();
				
			while(resultSet.next()) {
				int driverId = resultSet.getInt("driver_id");
				if(eventsMap.get(driverId) == null) {
					
					int truckId = resultSet.getInt("truck_id");
					String truckDriverEventKey = driverId + "|" + truckId;
					
					
					Timestamp eventTime = resultSet.getTimestamp("event_time");
					long eventTimeLong = eventTime.getTime();
					SimpleDateFormat sdf = new SimpleDateFormat();
					String timeStampString = sdf.format(eventTimeLong);						
					
					double latitude = resultSet.getDouble("latitude");
					double longitude = resultSet.getDouble("longitude");
					
					String lastInfraction = resultSet.getString("event_type");
					long numberOfInfractions = getInfractionCountForDriver(driverEventsCountTable, driverId);
					
					String driverName = resultSet.getString("driver_name");
					
					int routeId = resultSet.getInt("route_id");
					
					String routeName = resultSet.getString("route_name");	
					
					TruckDriverViolationEvent event = new TruckDriverViolationEvent(truckDriverEventKey, driverId, truckId, eventTimeLong, 
							timeStampString, longitude, latitude, lastInfraction, numberOfInfractions, driverName, routeId, routeName );
		
					eventsMap.put(driverId, event);					
					
				}
				
			}

			return eventsMap.values();
		} catch (Exception e) {
			LOG.error("Error getting driver events", e);
			throw new RuntimeException("Error getting driver events", e);
		} finally {
			
			try {
				statement.close();
				driverEventsCountTable.close();
				connection.close();
				phoenixConnection.close();
			} catch (Exception e) {
				//swallow
			}
		
		}
	}	
	
	/**
	 * Generates a stream of truck events
	 * @param params
	 * @param hdpServiceRegistry 
	 */
	public void generateTruckEventsStream(final StreamGeneratorParam params) {

		try {
			
			final Class eventEmitterClass = Class.forName(params.getEventEmitterClassName());
			final Class eventCollectorClass = Class.forName(params.getEventCollectorClassName());
			
			Config config= ConfigFactory.load();

			TruckConfiguration.initialize(params.getRouteDirectory());
			int emitters=TruckConfiguration.freeRoutePool.size();
			
			Thread.sleep(5000);
			
			ActorSystem system = ActorSystem.create("EventSimulator", config, getClass().getClassLoader());
			final ActorRef listener = system.actorOf(
					Props.create(SimulatorListener.class), "listener");
			
			String kafkaBrokerList = registry.getKafkaBrokerList();
			final ActorRef eventCollector = system.actorOf(
					Props.create(eventCollectorClass, kafkaBrokerList), "eventCollector");
			final int numberOfEmitters = emitters;
			
			final long demoId = new Random().nextLong();
			final ActorRef master = system.actorOf(new Props(
					new UntypedActorFactory() {
						public UntypedActor create() {
							return new SimulationMaster(
									numberOfEmitters,
									eventEmitterClass, listener, params.getNumberOfEvents(), demoId, params.getDelayBetweenEvents());
						}
					}), "master");
			master.tell(new StartSimulation(), master);
		} catch (Exception e) {
			throw new RuntimeException("Error running truck stream generator", e);
		} 
	
	}
	
	/*
	 * Resets the Trucking Demo Dataset which includes:
	 * 	1. Truncating HBase Tables: driver_dangerous_events, driver_dangerous_events_count
	 *  2. Truncating Phoenix Tables: dangerous_events
	 */
	public void resetDemo(DemoResetParam param) {
		if(param.isTruncateHbaseTables()) {
			truncateHBaseTables();
			//truncatePhoenixTables();
		}
	}

	
	/*
	 * Truncates a Phoenix table
	 */
	private void truncatePhoenixTables() {
		
		Connection phoenixConnection = null;		
		PreparedStatement statement = null;
		try {
			phoenixConnection = DriverManager.getConnection(registry.getPhoenixConnectionURL());
			statement = phoenixConnection.prepareStatement("delete from  truck.dangerous_events");
			statement.execute();
			
		} catch (Exception e) {
			LOG.error("Error truncating Phoenexing tables");
		} finally {
			if(phoenixConnection != null && statement != null) {
				try {
					statement.close();
					phoenixConnection.close();
				} catch (SQLException e) {
					LOG.error("Error closing connection", e);
				}
			}
		}
	}

	/*
	 * Truncates hbase tables associated with trucking demo
	 */
	public void truncateHBaseTables() {
		
		HConnection connection = null;
		HTable driverEventsTable = null;		
		HTable driverEventsCountTable = null;	
		HBaseAdmin admin = null;
		try {
			Configuration config = constructConfiguration();
			connection = HConnectionManager.createConnection(config);
			driverEventsTable = (HTable)connection.getTable(DRIVER_EVENTS_TABLE);
			driverEventsCountTable = (HTable) connection.getTable(DRIVER_EVENTS_COUNT_TABLE);
			
			admin = createHBaseAdmin(config);
			
			truncateTable(admin, driverEventsTable);
			truncateTable(admin, driverEventsCountTable);
		} catch (Exception  e) {
			LOG.error("Error truncating HBase tables", e);
			//do nothing
		} finally {
			try {
				driverEventsCountTable.close();
				driverEventsTable.close();
				admin.close();
				connection.close();
			} catch (Exception e) {
				//swallow
			}
		}
	}
	
	private void truncateTable(HBaseAdmin admin, HTable table)
			throws Exception{	
		
		HTableDescriptor tableDescriptor = table.getTableDescriptor();
		TableName tableName = table.getName();
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		admin.createTable(tableDescriptor, table.getStartKeys());
	}	
	

	
	private long getInfractionCountForDriver(HTableInterface driverEventsCountTable, int driverId) {
		
		try {
			long count = 0;
			byte[] driverCount = Bytes.toBytes(driverId);
			Get get = new Get(driverCount);
			Result result = driverEventsCountTable.get(get);
			byte[] countBytes = result.getValue(Bytes.toBytes(DRIVER_EVENTS_COUNT_COLUMN_FAMILY_NAME), Bytes.toBytes("incidentRunningTotal"));
			if(result!= null && countBytes != null) {
				count = Bytes.toLong(countBytes);
			} else {
				LOG.info("Driver["+driverId+"] return null for runnintTotalCount from Table[driver_dangerous_events_count]");
			}
			
			return count;
		} catch (Exception e) {
			LOG.error("Error getting infraction count", e);
			throw new RuntimeException("Error getting infraction count");
		}
	}

	private Configuration constructConfiguration() throws Exception {

		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum",
				registry.getHBaseZookeeperHost());
		config.set("hbase.zookeeper.property.clientPort", registry.getHBaseZookeeperClientPort());
		config.set("zookeeper.znode.parent", registry.getHBaseZookeeperZNodeParent());
		config.set("hbase.defaults.for.version.skip", "true");
		
		return config;
	}		
	
	private HBaseAdmin createHBaseAdmin(Configuration config) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(config);
		return admin;
	}
	

}
