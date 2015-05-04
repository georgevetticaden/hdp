package hortonworks.hdp.refapp.ecm.streaming.spout.kafka;

import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;
import hortonworks.hdp.refapp.ecm.util.DocumentGUIDUtils;

import java.util.List;

import org.apache.commons.lang3.SerializationUtils;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DocumentScheme implements Scheme {

	public List<Object> deserialize(byte[] docStream) {
		DocumentUpdateRequest docUpdateRequest = (DocumentUpdateRequest) SerializationUtils.deserialize(docStream);
		String docKey = DocumentGUIDUtils.generateDocGUID();
		byte[] docContent = docUpdateRequest.getDocumentContent();
		DocumentMetaData docMetaData = docUpdateRequest.getDocMetadata();
		return new Values(docKey, docContent, docMetaData);
		
	}

	public Fields getOutputFields() {
		return new Fields("docKey", "docContent", "docMetaData");
	}

}
