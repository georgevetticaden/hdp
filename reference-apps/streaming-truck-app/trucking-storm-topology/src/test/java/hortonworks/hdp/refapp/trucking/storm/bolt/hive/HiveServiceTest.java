package hortonworks.hdp.refapp.trucking.storm.bolt.hive;

import hortonworks.hdp.refapp.trucking.storm.bolt.hive.HiveTablePartitionHiveServer2Action;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.Test;

public class HiveServiceTest {
	
	
	private static final Logger LOG = Logger.getLogger(HiveServiceTest.class);
	

	
	
	//@Test
	public void testHiveServer2() throws Exception {
		String driverName = "org.apache.hive.jdbc.HiveDriver";

		Class.forName(driverName);

		// replace "hive" here with the name of the user the queries should run
		// as
		Connection con = DriverManager
				.getConnection(
						"jdbc:hive2://centralregion03.cloud.hortonworks.com:10000/default",
						"yarn", "");
		Statement stmt = con.createStatement();

		String path = "hdfs://centralregion01.cloud.hortonworks.com:8020/truck-events-v4/staging/truckEventshdfs_bolt-9-0-1420300308281.txt";
		String tableName2 = "truck_events_text_partition_single";

		String partitionValue = "2015-01-03-20";

		LOG.info("About to add file[" + path + "] to a partitions["
				+ partitionValue + "]");

		StringBuilder ddl = new StringBuilder();
		ddl.append(" load data inpath ").append(" '").append(path).append("' ")
				.append(" into table ").append(tableName2)
				.append(" partition ").append(" (date='")
				.append(partitionValue).append("')");

		String sql = ddl.toString();
		System.out.println("Running: " + sql);
		stmt.execute(sql);
	}
	

	//@Test
	public void testHiveTablePartitionHIveServer2Connection() throws Exception {
//		String driverName = "org.apache.hive.jdbc.HiveDriver";
//		Class.forName(driverName);		
		String hiveServer2ConnectionString = "jdbc:hive2://centralregion03.cloud.hortonworks.com:10000";
		String hiveServer2ConnectUser = "yarn";
		String tableName = "truck_events_text_partition_single";
		String databaseName = "default";
		String sourceFSUrl = "";
		HiveTablePartitionHiveServer2Action action = new HiveTablePartitionHiveServer2Action(hiveServer2ConnectionString, hiveServer2ConnectUser, tableName, databaseName, sourceFSUrl);
	}

}
