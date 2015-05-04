package hortonworks.hdp.refapp.ecm.streaming.kafka;

import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;
import hortonworks.hdp.refapp.ecm.util.DocumentSerializationUtils;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class DocumentSerializer implements Encoder<DocumentUpdateRequest> {

	public DocumentSerializer(VerifiableProperties props) {
		//Kafka needs this constructor for some reasons
	}
	
	public byte[] toBytes(DocumentUpdateRequest docUpdateRequest) {
		return DocumentSerializationUtils.serializeDocRequest(docUpdateRequest);
	}



}
