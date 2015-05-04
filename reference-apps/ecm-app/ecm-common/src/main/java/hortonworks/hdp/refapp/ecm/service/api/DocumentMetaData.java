package hortonworks.hdp.refapp.ecm.service.api;

import java.io.Serializable;

public class DocumentMetaData implements Serializable {
	

	private static final long serialVersionUID = 5549616292887506710L;

	private String documentName;
	private String mimeType;
	private DocumentClass docClassType;
	private String customerName;

	private String extension;
	
	
	
	public DocumentMetaData(String documentName, String mimeType,
			DocumentClass docClassType, String customerName, String extension) {
		super();
		this.documentName = documentName;
		this.mimeType = mimeType;
		this.docClassType = docClassType;
		this.customerName = customerName;
		this.extension = extension;
	}
	
	public String getDocumentName() {
		return documentName;
	}
	public void setDocumentName(String documentName) {
		this.documentName = documentName;
	}
	public DocumentClass getDocClassType() {
		return docClassType;
	}
	public void setDocClassType(DocumentClass docClassType) {
		this.docClassType = docClassType;
	}
	public String getCustomerName() {
		return customerName;
	}
	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}
	public String getMimeType() {
		return mimeType;
	}
	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}
	
	
	
	
}
