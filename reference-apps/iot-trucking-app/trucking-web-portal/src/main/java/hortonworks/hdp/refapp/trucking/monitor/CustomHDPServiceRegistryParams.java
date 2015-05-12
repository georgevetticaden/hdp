package hortonworks.hdp.refapp.trucking.monitor;

import hortonworks.hdp.apputil.registry.ServiceRegistryParams;

public class CustomHDPServiceRegistryParams extends ServiceRegistryParams {

	private String activeMQHost;
	
	private String solrServerUrl;
	
	
	public String getActiveMQHost() {
		return activeMQHost;
	}
	
	public void setActiveMQHost(String activeMQHost) {
		this.activeMQHost = activeMQHost;
	}
	
	public String getSolrServerUrl() {
		return solrServerUrl;
	}
	
	public void setSolrServerUrl(String solrServerUrl) {
		this.solrServerUrl = solrServerUrl;
	}
	
	
}
