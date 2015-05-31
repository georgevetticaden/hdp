package hortonworks.hdp.apputil.hbase;


import java.util.List;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

public class HBaseUtils {
	
	private HDPServiceRegistry registry;
	
	private static final Logger LOG = Logger.getLogger(HBaseUtils.class);

	public HBaseUtils(HDPServiceRegistry registry) {
		this.registry = registry;
	}
	
	public void createHBaseTable(String tableName, List<String> columnFamilies) throws Exception {
		Configuration config = constructConfiguration();
		HConnection connection = HConnectionManager.createConnection(config);
		HBaseAdmin admin = createHBaseAdmin(config);
		
		HTableDescriptor desc = new HTableDescriptor(tableName);
		
		for(String columnFamily: columnFamilies) {
			HColumnDescriptor meta = new HColumnDescriptor(columnFamily.getBytes());
			desc.addFamily(meta);			
		}

		admin.createTable(desc);	
		connection.close();
	}	
	
	public void createHBaseTable(String tableName, String columnFamily) throws Exception {
		Configuration config = constructConfiguration();
		HConnection connection = HConnectionManager.createConnection(config);
		HBaseAdmin admin = createHBaseAdmin(config);
		
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor meta = new HColumnDescriptor(columnFamily.getBytes());
		desc.addFamily(meta);
		admin.createTable(desc);	
		
		connection.close();
	}
	

	public void truncateHBaseTables(List<String> tablesNames) {
		
		HConnection connection = null;
		HBaseAdmin admin = null;
		try {
			Configuration config = constructConfiguration();
			connection = HConnectionManager.createConnection(config);
			admin = createHBaseAdmin(config);
			
			for(String tableName: tablesNames) {
				HTable table = null;
				try {
					table = (HTable)connection.getTable(tableName);	
					truncateTable(admin, table);					
				} catch (Exception e) {
					LOG.error("Error truncating HBase table[" + tableName + "]", e);
				} finally {
					table.close();
				}

			}

		} catch (Exception  e) {
			LOG.error("Error truncating HBase tables", e);
			//do nothing
		} finally {
			try {
				
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
	
	private HBaseAdmin createHBaseAdmin(Configuration config) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(config);
		return admin;
	}

	private Configuration constructConfiguration() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				registry.getHBaseZookeeperHost());
		config.set("hbase.zookeeper.property.clientPort",registry.getHBaseZookeeperClientPort());
		config.set("zookeeper.znode.parent", registry.getHBaseZookeeperZNodeParent());
		return config;
	}	

}
