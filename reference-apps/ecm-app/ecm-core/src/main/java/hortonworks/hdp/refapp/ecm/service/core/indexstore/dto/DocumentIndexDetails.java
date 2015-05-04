package hortonworks.hdp.refapp.ecm.service.core.indexstore.dto;

import java.io.Serializable;

public class DocumentIndexDetails implements Serializable {

	private static final long serialVersionUID = 8757656970073559684L;

	private String docKey;
	private byte[] docContent;
	
	private IndexMetaData indexMetaData;
	
	
	public DocumentIndexDetails(String docKey, byte[] docContent,
			IndexMetaData indexMetaData) {
		super();
		this.docKey = docKey;
		this.docContent = docContent;
		this.indexMetaData = indexMetaData;
	}

	public String getDocKey() {
		return this.docKey;
	}

	public byte[] getDocContent() {
		return docContent;
	}

	public void setDocContent(byte[] docContent) {
		this.docContent = docContent;
	}

	public void setDocKey(String docKey) {
		this.docKey = docKey;
	}

	public IndexMetaData getIndexMetaData() {
		return indexMetaData;
	}

	public void setIndexMetaData(IndexMetaData indexMetaData) {
		this.indexMetaData = indexMetaData;
	}
	
	
	

}
