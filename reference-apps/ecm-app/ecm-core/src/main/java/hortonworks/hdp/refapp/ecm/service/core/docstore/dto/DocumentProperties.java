package hortonworks.hdp.refapp.ecm.service.core.docstore.dto;

import java.io.Serializable;


public class DocumentProperties implements Serializable {


	private static final long serialVersionUID = 1336891589842142241L;
	
	private String name;
	private String mimeType;
	private String extension;

	
	
	public DocumentProperties(String name, String mimeType, String extension) {
		super();
		this.name = name;
		this.mimeType = mimeType;
		this.extension = extension;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
