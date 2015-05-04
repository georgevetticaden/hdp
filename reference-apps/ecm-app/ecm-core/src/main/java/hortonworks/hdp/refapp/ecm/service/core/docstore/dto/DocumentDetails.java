package hortonworks.hdp.refapp.ecm.service.core.docstore.dto;

import java.io.Serializable;

public class DocumentDetails implements Serializable {
	

	private static final long serialVersionUID = -6411087130312531003L;

	private String docKey;
	private byte[] docContent;
	private DocumentProperties docProperties;

	
	
	public DocumentDetails(String docKey, byte[] docContent,
			DocumentProperties docProperties) {
		super();
		this.docKey = docKey;
		this.docContent = docContent;
		this.docProperties = docProperties;
	}

	public byte[] getDocContent() {
		return docContent;
	}

	public void setDocContent(byte[] docContent) {
		this.docContent = docContent;
	}

	public DocumentProperties getDocProperties() {
		return docProperties;
	}

	public void setDocProperties(DocumentProperties docProperties) {
		this.docProperties = docProperties;
	}

	public String getDocKey() {
		return docKey;
	}

	public void setDocKey(String docKey) {
		this.docKey = docKey;
	}
	
	
	
	
	

}
