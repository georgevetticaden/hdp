package hortonworks.hdp.refapp.ecm.util;

import hortonworks.hdp.apputil.hbase.HBaseUtils;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.service.core.docstore.HBaseDocumentStore;

import java.util.ArrayList;
import java.util.List;

public class DocumentStoreUtils {
	
	public static void truncate(HDPServiceRegistry serviceRegistry) {
		HBaseUtils hbaseUtils = new HBaseUtils(serviceRegistry);
		List<String> tableNames = new ArrayList<String>();
		tableNames.add(HBaseDocumentStore.IMAGE_STORE_TABLE_NAME);
		hbaseUtils.truncateHBaseTables(tableNames);
		
	}	
	
	public static void createTables(HDPServiceRegistry serviceRegistry) throws Exception {
		HBaseUtils hbaseUtils = new HBaseUtils(serviceRegistry);
		
		List<String> columnFamilies = new ArrayList<String>();
		columnFamilies.add(HBaseDocumentStore.IMAGE_STORE_DOC_CONTENT_COLUMN_FAMILY);
		columnFamilies.add(HBaseDocumentStore.IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY);
		hbaseUtils.createHBaseTable(HBaseDocumentStore.IMAGE_STORE_TABLE_NAME, columnFamilies);
	}

}
