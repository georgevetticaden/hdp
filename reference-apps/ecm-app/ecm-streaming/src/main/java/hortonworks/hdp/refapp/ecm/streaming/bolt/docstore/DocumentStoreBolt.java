package hortonworks.hdp.refapp.ecm.streaming.bolt.docstore;

import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentDetails;
import hortonworks.hdp.refapp.ecm.service.core.docstore.dto.DocumentProperties;
import hortonworks.hdp.refapp.ecm.streaming.bolt.BaseECMBolt;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class DocumentStoreBolt extends BaseECMBolt {

	private static final long serialVersionUID = -44097374613249103L;
	private static final Logger LOG = Logger.getLogger(DocumentStoreBolt.class);
	
	private DocumentStore docStore;


	public DocumentStoreBolt(Properties topologyConfig) {
		super(topologyConfig);

	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
		LOG.info("Preparing DocumentStoreBolt by Creating AppContext and getting DocumentStore");
		this.docStore = createAppContext().getBean(DocumentStore.class);
		LOG.info("Finished Preparing DocumentStoreBolt by Creating AppContext and getting DocumentStore");
	}

	public void execute(Tuple input) {
		DocumentDetails docDetails = constructDocDetails(input);
		try {
			
			LOG.info("Storing Doc with Key["+docDetails.getDocKey() + "] into the Document Store");
			docStore.storeDocument(docDetails);
			LOG.info("Finished Storing Doc with Key["+docDetails.getDocKey() + "] into the Document Store");
			
		} catch (Exception e) {
			String errMsg = "Error inserting Doc with Key["+ docDetails.getDocKey() + "]  and Name["+docDetails.getDocProperties().getName() + "] into image store";
			LOG.error(errMsg, e);
		}
		//TODO: Might need to revisit: still acknowledge if error
		collector.ack(input);		
		
	}

	private DocumentDetails constructDocDetails(Tuple input) {
		String docKey = (String) input.getValueByField("docKey");
		byte[] docContent = (byte[]) input.getValueByField("docContent");
		DocumentMetaData docMetaData = (DocumentMetaData) input.getValueByField("docMetaData");
		DocumentProperties docProperties = new DocumentProperties(docMetaData.getDocumentName(), docMetaData.getMimeType(), docMetaData.getExtension());
		DocumentDetails docDetails = new DocumentDetails(docKey, docContent, docProperties);
		return docDetails;
	}

	

	
}
