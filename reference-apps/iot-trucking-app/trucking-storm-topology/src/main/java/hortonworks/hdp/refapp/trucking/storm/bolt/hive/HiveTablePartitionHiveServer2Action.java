package hortonworks.hdp.refapp.trucking.storm.bolt.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.hdfs.common.rotation.RotationAction;

/**
 * When an HDFS File Rotation policy executes, this action will create hive partition based on the timestamp of the file and load the hdfs file into the partition
 * @author gvetticaden
 *
 */
public class HiveTablePartitionHiveServer2Action implements RotationAction {


	private static final long serialVersionUID = 2725320320183384402L;
	
	private static final Logger LOG = LoggerFactory.getLogger(HiveTablePartitionHiveServer2Action.class);
	
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	private String databaseName;
	private String tableName;
	private String sourceFSrl;

	private String hiveServer2ConnectionString;
	private String hiveServer2ConnectUser;

	
	
	
	public HiveTablePartitionHiveServer2Action(String hiveServer2ConnectionString, String hiveServer2ConnectUser, String tableName, String databaseName, String sourceFSUrl) {
		super();
		this.tableName = tableName;
		this.databaseName = databaseName;
		this.sourceFSrl = sourceFSUrl;
		this.dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		this.dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		this.hiveServer2ConnectionString = hiveServer2ConnectionString;
		this.hiveServer2ConnectUser = hiveServer2ConnectUser;
		
		initializeHiveServer2Driver();

		
	}

	private void initializeHiveServer2Driver() {
		String driverName = "org.apache.hive.jdbc.HiveDriver";
		try {
			Class.forName(driverName);
		} catch (Exception e1) {
			String errMsg = "Driver Cannot be found for HiveServer2["+driverName+"]";
			LOG.error(errMsg);
			throw new RuntimeException(errMsg, e1);
		}	
	}

	@Override
	public void execute(FileSystem fileSystem, Path filePath)
			throws IOException {
		
		long timeStampFromFile= getTimestamp(filePath.getName());
		Date date = new Date(timeStampFromFile);
		
		
		String datePartitionName = constructDatePartitionName(date);
		String hourPartitionName = constructHourPartitionName(date);
		
		String fileNameWithSchema = sourceFSrl + filePath.toString();		
		
		addFileToPartition(fileNameWithSchema, datePartitionName, hourPartitionName);

	}
	
    private String constructHourPartitionName(Date date) {
		Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		calendar.setTime(date);
		int hour =  calendar.get(Calendar.HOUR_OF_DAY);
		if(hour< 10) {
			return "0"+hour;
		} else {
			return String.valueOf(hour);
		}
		
	}

	private String constructDatePartitionName(Date date) {
		return dateFormat.format(date);
	}

	private long getTimestamp(String fileName) {
		int startIndex = fileName.lastIndexOf("-");
		int endIndex = fileName.lastIndexOf(".");
		String timeStamp = fileName.substring(startIndex + 1, endIndex);
		return Long.valueOf(timeStamp);
	}

	private void addFileToPartition(String fileNameWithSchema, String datePartitionName, String hourPartitionName ) {
         
        loadData(fileNameWithSchema, datePartitionName, hourPartitionName);
        
    }	
    
    public void loadData(String path, String datePartitionName, String hourPartitionName ) {
    	
    	initializeHiveServer2Driver();
    	
    	String partitionValue = datePartitionName + "-" + hourPartitionName;
    	
    	LOG.info("About to add file["+ path + "] to a partitions["+partitionValue + "]");
    	
    	StringBuilder ddl = new StringBuilder();
    	ddl.append(" load data inpath ")
			.append(" '").append(path).append("' ")
			.append(" into table ")
			.append(tableName)
			.append(" partition ").append(" (dateTruck='").append(partitionValue).append("')");

    	String hiveServer2ConnectionStringWithDB = hiveServer2ConnectionString + "/" + this.databaseName;
    	Connection hiveServer2Connection = null;
    	Statement sqlLoadStmt = null;
   		 try {
   			hiveServer2Connection = DriverManager.getConnection(hiveServer2ConnectionStringWithDB, this.hiveServer2ConnectUser, "");
   			sqlLoadStmt = hiveServer2Connection.createStatement();
   			sqlLoadStmt.execute(ddl.toString());
   		} catch (SQLException e) {
   			String errorMsg = "Error connecting to HiveServer2 with connection url["+ hiveServer2ConnectionString + "] with user["+ this.hiveServer2ConnectUser + "]";
   			LOG.error(errorMsg);
   			throw new RuntimeException(errorMsg, e);
   		} catch (Exception e) {
			String errorMessage = "Error exexcuting query["+ddl.toString() + "]";
			LOG.error(errorMessage, e);
			throw new RuntimeException(errorMessage, e);
		} finally {
			if(sqlLoadStmt != null) {
					try {
						sqlLoadStmt.close();
					} catch (SQLException e) {
						LOG.error("Error executing Hive Statement", e);
						
					}
			}
			if(hiveServer2Connection != null) {
				try {
					hiveServer2Connection.close();
				} catch (SQLException e) {
					LOG.error("Error executing Hive Statement", e);
				}
			}
		}
    }    
    

}
