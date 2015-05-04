package hortonworks.hdp.refapp.ecm.util;

import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;

import org.apache.commons.lang3.SerializationUtils;

public final class DocumentSerializationUtils {
	
	public static final DocumentUpdateRequest deserializedDocRequest(byte[] bytes) {
		DocumentUpdateRequest docUpdateRequest = (DocumentUpdateRequest) SerializationUtils.deserialize(bytes);
		return docUpdateRequest;
	}
	
	public static final byte[] serializeDocRequest(DocumentUpdateRequest docUpdateRequest) {
		return SerializationUtils.serialize(docUpdateRequest);
	}

}
