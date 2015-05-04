package hortonworks.hdp.refapp.ecm.service.api;

public enum DocumentClass {
	RFP("rfp");
	
	
	private String value;

	private DocumentClass(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return this.value;
	}
}
