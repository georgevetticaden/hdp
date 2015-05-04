package hortonworks.hdp.refapp.ecm.service.core.indexstore.repository;


import java.util.Date;

import org.apache.solr.client.solrj.beans.Field;

public class RFIDocument {
	
	@Field
	private String id;
	
	
	@Field
	private String customername;
	
	@Field
	private String documentclass;
	
	@Field
	private String mimetype;
	
	@Field
	private String documentname;
	
	@Field
	private String body;
	
	@Field
	public String last_author;
	
	@Field
	public Date date;
	
	@Field
	public String publisher;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}


	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	public String getCustomername() {
		return customername;
	}

	public void setCustomername(String customername) {
		this.customername = customername;
	}

	public String getDocumentclass() {
		return documentclass;
	}

	public void setDocumentclass(String documentclass) {
		this.documentclass = documentclass;
	}

	public String getDocumentname() {
		return documentname;
	}

	public void setDocumentname(String documentname) {
		this.documentname = documentname;
	}

	public String getMimetype() {
		return mimetype;
	}

	public void setMimetype(String mimetype) {
		this.mimetype = mimetype;
	}

	public String getLast_author() {
		return last_author;
	}

	public void setLast_author(String last_author) {
		this.last_author = last_author;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	
	

}
