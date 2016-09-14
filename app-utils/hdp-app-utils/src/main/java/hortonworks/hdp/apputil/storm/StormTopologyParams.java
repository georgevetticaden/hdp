package hortonworks.hdp.apputil.storm;

import java.util.Properties;

import org.apache.storm.generated.StormTopology;

public class StormTopologyParams {

	private boolean upload;
	private String uploadedTopologyJarLocation;
	private String topologyJarLocation;
	private String topologyName;
	private StormTopology topology;
	private int numberOfWorkers;
	private Integer eventLogExecutors;
	private Integer topologyMessageTimeoutSecs;
	
	private Properties customStormProperties;
	

	public Integer getTopologyMessageTimeoutSecs() {
		return topologyMessageTimeoutSecs;
	}


	public Integer getEventLogExecutors() {
		return eventLogExecutors;
	}


	public void setEventLogExecutors(Integer eventLogExecutors) {
		this.eventLogExecutors = eventLogExecutors;
	}


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




	public int getNumberOfWorkers() {
		return numberOfWorkers;
	}


	public void setNumberOfWorkers(int numberOfWorkers) {
		this.numberOfWorkers = numberOfWorkers;
	}


	public void setTopologyEventLogExecutors(Integer executors) {
		this.eventLogExecutors = executors;
		
	}


	public void setTopologyMessageTimeoutSecs(Integer timeout) {
		this.topologyMessageTimeoutSecs = timeout;
	}


	public Properties getCustomStormProperties() {
		return customStormProperties;
	}


	public void setCustomStormProperties(Properties customStormProperties) {
		this.customStormProperties = customStormProperties;
	}
	
	

}
