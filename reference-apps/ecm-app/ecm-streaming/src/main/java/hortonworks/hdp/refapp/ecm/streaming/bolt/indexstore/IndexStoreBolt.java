package hortonworks.hdp.refapp.ecm.streaming.bolt.indexstore;

import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentIndexDetails;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.IndexMetaData;
import hortonworks.hdp.refapp.ecm.streaming.bolt.BaseECMBolt;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;


public class IndexStoreBolt extends BaseECMBolt {

	
	private static final long serialVersionUID = 3129752334232165171L;

	private static final Logger LOG = Logger.getLogger(IndexStoreBolt.class);
	
	private IndexStore indexStore = null;

	
	public IndexStoreBolt(Properties topologyConfig) {
		super(topologyConfig);	
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
		LOG.info("Preparing IndexStoreBolt by Creating AppContext and getting IndexStore");
		this.indexStore = createAppContext().getBean(IndexStore.class);
		LOG.info("Finished Preparing IndexStoreBolt by Creating AppContext and getting IndexStore");
	}

	
	public void execute(Tuple input) {
		DocumentIndexDetails indexDetails = constructDocIndexDetails(input);
		try {
			
			LOG.info("Indexing Doc with Key["+indexDetails.getDocKey() + "] into the Index Store");
			indexStore.addDocument(indexDetails);
			LOG.info("Finished Indexing Doc with Key["+indexDetails.getDocKey() + "] into the Index Store");
			
			
		} catch (Exception e) {
			String errMsg = "Error inserting Doc with Key["+ indexDetails.getDocKey() + "] and Name["+indexDetails.getIndexMetaData().getDocumentName() + "] into image store";
			LOG.error(errMsg, e);
		}
		
		
		//TODO: Might need to revisit: still acknowledge if error
		collector.ack(input);
		
	}
	
	private DocumentIndexDetails constructDocIndexDetails(Tuple input) {
		String docKey = (String) input.getValueByField("docKey");
		byte[] docContent = (byte[]) input.getValueByField("docContent");
		DocumentMetaData docMetadata = (DocumentMetaData) input.getValueByField("docMetaData");
		IndexMetaData indexMetaData = constructIndexMetaData(docMetadata);
		return new DocumentIndexDetails(docKey, docContent, indexMetaData);
	}	
	
	private IndexMetaData constructIndexMetaData(DocumentMetaData docMetadata) {
		IndexMetaData indexMetaData = new IndexMetaData(docMetadata.getDocumentName(), docMetadata.getMimeType(), docMetadata.getDocClassType(), docMetadata.getCustomerName());
		return indexMetaData;
	}	

	
		
	

}
