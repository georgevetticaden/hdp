package hortonworks.hdp.refapp.ecm.service.core.docstore;


import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentProperties;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.apache.hadoop.conf.Configuration;

public class HBaseDocumentStore implements DocumentStore {

	private static final long serialVersionUID = -2140894288704637001L;

	public static final String IMAGE_STORE_TABLE_NAME = "document_store";
	
	public static final String IMAGE_STORE_DOC_CONTENT_COLUMN_FAMILY = "doc_content";
	public static final String IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY = "doc_properties";
	
	public static final String IMAGE_STORE_DOC_CONTENT_DOCUMENT_COLUMN = "document";
	public static final String IMAGE_STORE_DOC_PROPERTIES_NAME_COLUMN = "name";
	public static final String IMAGE_STORE_DOC_PROPERTIES_MIME_TYPE_COLUMN = "mimetype";
	public static final String IMAGE_STORE_DOC_PROPERTIES_EXTENSION_COLUMN = "extension";
	
	
	private HbaseTemplate hbaseTemplate;
	private HDPServiceRegistry serviceRegistry;
	


	public HBaseDocumentStore(HDPServiceRegistry registry) {
		this.serviceRegistry = registry;
	}

	public void initialize() {
		Configuration  config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				serviceRegistry.getHBaseZookeeperHost());
		config.set("hbase.zookeeper.property.clientPort",serviceRegistry.getHBaseZookeeperClientPort());
		config.set("zookeeper.znode.parent", serviceRegistry.getHBaseZookeeperZNodeParent());
		
		this.hbaseTemplate = new HbaseTemplate(config);

	}

	/**
	 * Stores the Document and its properties into HBase Document Store
	 */
	public void storeDocument(final DocumentDetails docDetails) throws Exception {
		
		hbaseTemplate.execute(IMAGE_STORE_TABLE_NAME, new TableCallback<String>() {

			@Override
			public String doInTable(HTableInterface hTable) throws Throwable {
				Put put = constructRow(docDetails);
				hTable.put(put);
				return docDetails.getDocKey();
			}
		});
	}
	

	/**
	 * Retrives the Document and its Document Properties from tge HBase Document Store
	 */
	@Override
	public DocumentDetails getDocument(final String key) throws Exception {
		
		if(StringUtils.isEmpty(key)) {
			String errMsg = "Key cannot be null when retreieving Doc";
			throw new RuntimeException(errMsg);
		}
		
		RowMapper<DocumentDetails> rowMapper = new RowMapper<DocumentDetails>() {

			@Override
			public DocumentDetails mapRow(Result result, int rowNum) {
				DocumentDetails docDetails = null;
				if(result != null && result.getRow() != null ) {
					
					byte[] docContent = result.getValue(Bytes.toBytes(IMAGE_STORE_DOC_CONTENT_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_CONTENT_DOCUMENT_COLUMN));
					byte[] docName = result.getValue(Bytes.toBytes(IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_PROPERTIES_NAME_COLUMN));
					byte[] mimeType = result.getValue(Bytes.toBytes(IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_PROPERTIES_MIME_TYPE_COLUMN));
					byte[] extension = result.getValue(Bytes.toBytes(IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_PROPERTIES_EXTENSION_COLUMN));
					
					
					DocumentProperties docProperties = new DocumentProperties(Bytes.toString(docName), Bytes.toString(mimeType), Bytes.toString(extension));
					
					docDetails = new DocumentDetails(key, docContent, docProperties);
				}
					
				return docDetails;
			}
		};
		
		return hbaseTemplate.get(IMAGE_STORE_TABLE_NAME, key, rowMapper);
	}	
	
	@Override
	public List<String> getAllDocumentKeys() throws Exception {
		
		RowMapper<String> rowMapper = new RowMapper<String>() {

			@Override
			public String mapRow(Result result, int rowNum) throws Exception {
				return Bytes.toString(result.getRow());
			}
		};
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		List<String> docKeys = hbaseTemplate.find(IMAGE_STORE_TABLE_NAME, scan, rowMapper);
		if(docKeys == null) {
			docKeys = new ArrayList<String>();
		}
		return docKeys;
	}	
	
	private Put constructRow(DocumentDetails docDetails) throws Exception {
		
		Put put = new Put(Bytes.toBytes(docDetails.getDocKey()));
		put.add(Bytes.toBytes(IMAGE_STORE_DOC_CONTENT_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_CONTENT_DOCUMENT_COLUMN), docDetails.getDocContent());
		DocumentProperties docProperties = docDetails.getDocProperties();
		if(docProperties != null) {
			put.add(Bytes.toBytes(IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_PROPERTIES_NAME_COLUMN), docProperties.getName().getBytes());
			put.add(Bytes.toBytes(IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_PROPERTIES_MIME_TYPE_COLUMN), docProperties.getMimeType().getBytes());
			put.add(Bytes.toBytes(IMAGE_STORE_DOC_PROPERITES_COLUMN_FAMILY), Bytes.toBytes(IMAGE_STORE_DOC_PROPERTIES_EXTENSION_COLUMN), docProperties.getExtension().getBytes());
		}
		
		return put;
	}





}
