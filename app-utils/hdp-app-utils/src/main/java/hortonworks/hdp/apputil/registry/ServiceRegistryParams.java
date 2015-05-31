package hortonworks.hdp.apputil.registry;


public class ServiceRegistryParams {
	
	private String ambariUrl;
	private String clusterName;
	
	private DeploymentMode hbaseDeploymentMode;
	private String hbaseSliderPublisherUrl;
	
	private DeploymentMode stormDeploymentMode;
	private String stormSliderPublisherUrl;

	
	public String getAmbariUrl() {
		return ambariUrl;
	}

	public void setAmbariUrl(String ambariUrl) {
		this.ambariUrl = ambariUrl;
	}

	public String getClusterName() {
		return this.clusterName;
	}

	public void setClusterName(String clusterName) {
		
		this.clusterName = clusterName;
	}

	public String getHbaseSliderPublisherUrl() {
		return this.hbaseSliderPublisherUrl;
	}

	public void setHbaseSliderPublisherUrl(String hbaseSliderPublisherUrl) {
		this.hbaseSliderPublisherUrl = hbaseSliderPublisherUrl;
	}

	public String getStormSliderPublisherUrl() {
		return stormSliderPublisherUrl;
	}

	public void setStormSliderPublisherUrl(String stormSliderPublisherUrl) {
		this.stormSliderPublisherUrl = stormSliderPublisherUrl;
	}

	public DeploymentMode getHbaseDeploymentMode() {
		return hbaseDeploymentMode;
	}

	public void setHbaseDeploymentMode(DeploymentMode hbaseDeploymentMode) {
		this.hbaseDeploymentMode = hbaseDeploymentMode;
	}

	public DeploymentMode getStormDeploymentMode() {
		return stormDeploymentMode;
	}

	public void setStormDeploymentMode(DeploymentMode stormDeploymentMode) {
		this.stormDeploymentMode = stormDeploymentMode;
	}
	
	
}
