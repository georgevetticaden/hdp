package hortonworks.hdp.refapp.ecm.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import hortonworks.hdp.refapp.ecm.BaseTest;
import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;
import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;

import org.junit.Test;

public class DocumentSerializationUtilsTest extends BaseTest {
	
	@Test
	public void testSerializeAndDeserialize() throws Exception{
		DocumentUpdateRequest requestToSerialize = constructDocumentUpdateRequest();
		byte[] requestSerialized = DocumentSerializationUtils.serializeDocRequest(requestToSerialize);
		assertNotNull(requestSerialized);
		
		DocumentUpdateRequest deserialialzedRequest = DocumentSerializationUtils.deserializedDocRequest(requestSerialized);
		assertNotNull(deserialialzedRequest);
		assertNotNull(deserialialzedRequest.getDocMetadata());
		assertNotNull(deserialialzedRequest.getDocMetadata());
		assertNull(deserialialzedRequest.getDocKey());
		
		DocumentMetaData deserialisedDocMetaData = deserialialzedRequest.getDocMetadata();
		assertThat(deserialisedDocMetaData.getDocumentName(), is("State Farm RFP"));
		
	}
	
	private DocumentUpdateRequest constructDocumentUpdateRequest() throws Exception {
		
		//generate request
		byte[] document = getTestDocument("rfi/HortonworksRFIResponsetoUSCellular.pdf");
		

		DocumentMetaData docMetaData = createDocMetadataForStateFarmRFP();
		DocumentUpdateRequest docUpdateRequest = new DocumentUpdateRequest(null, document, docMetaData);

		return docUpdateRequest;
	}	

	private DocumentMetaData createDocMetadataForStateFarmRFP() {
		String customerName = "StateFarm";
		String documentName = "State Farm RFP";
		String mimeType = "application/pdf";
		String extension = ".pdf";
		DocumentClass docClassType = DocumentClass.RFP;
		DocumentMetaData docMeta = new DocumentMetaData(documentName, mimeType, docClassType, customerName, extension);
		return docMeta;
	}		

}
