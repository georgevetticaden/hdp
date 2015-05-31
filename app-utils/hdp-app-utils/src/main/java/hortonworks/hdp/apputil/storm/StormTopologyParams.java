package hortonworks.hdp.apputil.storm;

import backtype.storm.generated.StormTopology;

public class StormTopologyParams {

	private boolean upload;
	private String uploadedTopologyJarLocation;
	private String topologyJarLocation;
	private String topologyName;
	private StormTopology topology;
	

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}


	public boolean isUpload() {
		return upload;
	}


	public void setUpload(boolean upload) {
		this.upload = upload;
		
	}


	public void setUploadedTopologyJarLocation(String value) {
		this.uploadedTopologyJarLocation = value;
		
	}


	public String getUploadedTopologyJarLocation() {
		return uploadedTopologyJarLocation;
	}


	public String getTopologyJarLocation() {
		return this.topologyJarLocation;
	}
	
	public void setTopologyJarLocation(String topologyParams) {
		this.topologyJarLocation = topologyParams;
	}


	public String getTopologyName() {
		return this.topologyName;
	}


	public StormTopology getTopology() {
		return topology;
	}


	public void setTopology(StormTopology topology) {
		this.topology = topology;
	}
	
	

}
