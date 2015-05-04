package hortonworks.hdp.refapp.ecm.service.api;

import java.io.Serializable;

public class DocumentUpdateRequest implements Serializable{

	private static final long serialVersionUID = 5726796929653644530L;
	
	private String docKey;
	private byte[] documentContent;
	private DocumentMetaData docMetadata;
	
	public DocumentUpdateRequest(String docKey, byte[] documentContent,
			DocumentMetaData docMetadata) {
		super();
		this.docKey = docKey;
		this.documentContent = documentContent;
		this.docMetadata = docMetadata;
	}
	public byte[] getDocumentContent() {
		return documentContent;
	}
	public void setDocumentContent(byte[] documentContent) {
		this.documentContent = documentContent;
	}
	public DocumentMetaData getDocMetadata() {
		return docMetadata;
	}
	public void setDocMetadata(DocumentMetaData docMetadata) {
		this.docMetadata = docMetadata;
	}
	public String getDocKey() {
		return docKey;
	}
	public void setDocKey(String docKey) {
		this.docKey = docKey;
	}	
	
	
	
}
