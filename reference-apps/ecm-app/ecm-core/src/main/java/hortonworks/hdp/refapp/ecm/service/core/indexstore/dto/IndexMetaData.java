package hortonworks.hdp.refapp.ecm.service.core.indexstore.dto;

import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;

import java.io.Serializable;

public class IndexMetaData implements Serializable {

	
	private String documentName;
	private String mimeType;
	private DocumentClass docClassType;
	
	
	public IndexMetaData(String documentName, String mimeType,
			DocumentClass docClassType, String customerName) {
		super();
		this.documentName = documentName;
		this.mimeType = mimeType;
		this.docClassType = docClassType;
		this.customerName = customerName;
	}
	
	private String customerName;
	public String getDocumentName() {
		return documentName;
	}
	public void setDocumentName(String documentName) {
		this.documentName = documentName;
	}
	public String getMimeType() {
		return mimeType;
	}
	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
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
	

}
