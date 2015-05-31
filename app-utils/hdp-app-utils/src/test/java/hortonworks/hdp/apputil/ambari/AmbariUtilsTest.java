package hortonworks.hdp.apputil.ambari;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

public class AmbariUtilsTest {
	
	private static final String AMBARI_URL = "http://centralregion01.cloud.hortonworks.com:8080/api/v1/clusters/centralregioncluster";
	
	private static final Logger LOG = Logger.getLogger(AmbariUtils.class);
	AmbariUtils ambariService = new AmbariUtils(AMBARI_URL); 
		

	//@Test
	public void getHBaseZookeeperQuorum() throws Exception {
		List<String> quorumList = ambariService.getHBaseZookeeperQuorum();
		assertThat(quorumList.size(), is(3));
		assertTrue(quorumList.contains("vett-cluster01.cloud.hortonworks.com"));
		assertTrue(quorumList.contains("vett-cluster02.cloud.hortonworks.com"));
		assertTrue(quorumList.contains("vett-cluster03.cloud.hortonworks.com"));
			
	}
	
	//@Test
	public void getHBaseZookeeperCientPort() throws Exception {
		assertThat(ambariService.getHBaseZookepperCientPort(), is("2181"));
	}
	
	//@Test
	public void getHBaseZookeeperParentNode() throws Exception {
		assertThat(ambariService.getHBaseZookeeperParentNode(), is("/hbase-unsecure"));
	}	
	
	//@Test
	public void getStormZookeeperQuorum() throws Exception{
		assertThat(ambariService.getStormZookeeperQuorum(), is("['vett-cluster01.cloud.hortonworks.com','vett-cluster02.cloud.hortonworks.com','vett-cluster03.cloud.hortonworks.com']"));
	}
	
	//@Test
	public void getStormNimbusPort() throws Exception{
		assertThat(ambariService.getStormNimbusPort(), is("6627"));
	}	
	
	//@Test
	public void getStormNimbusHost() throws Exception{
		assertThat(ambariService.getStormNimbusHost(), is("vett-cluster02.cloud.hortonworks.com"));
	}	
	
	//@Test
	public void getStormUIServerHost() throws Exception {
		assertThat(ambariService.getStormUIServer(), is("vett-cluster02.cloud.hortonworks.com"));
	}
	
	//@Test
	public void getStormUIPort() throws Exception {
		assertThat(ambariService.getStormUIPort(), is("8744"));
	}	

	@Test
	public void getHDFSUrl() throws Exception{
		assertThat(ambariService.getHDFSUrl(), is("hdfs://centralregion01.cloud.hortonworks.com:8020"));
	}	

	@Test
	public void getHiveMetaStoreUrl() throws Exception{
		assertThat(ambariService.getHiveMetaStoreUrl(), is("thrift://centralregion03.cloud.hortonworks.com:9083"));
	}	
	
	@Test
	public void getHiveServer2Host() throws Exception {
		assertThat(ambariService.getHiveServer2Host(), is("centralregion03.cloud.hortonworks.com"));
	}	
	
	@Test
	public void getHiveServer2ThriftPort() throws Exception {
		assertThat(ambariService.getHiveServer2ThriftPort(), is("10000"));
	}		
	
	@Test
	public void getFalconHost() throws Exception{
		assertThat(ambariService.getFalconHost(), is("centralregion03.cloud.hortonworks.com"));
	}		
	
	@Test
	public void getFalconBrokerUrl() throws Exception {
		assertThat(ambariService.getFalconBrokerUrl(), is("tcp://centralregion03.cloud.hortonworks.com:61616"));
	}
	
	@Test
	public void getResourceManagerURL() throws Exception {
		assertThat(ambariService.getResourceManagerUrl(), is("centralregion02.cloud.hortonworks.com:8050"));
	}
	
	@Test
	public void getResourceManagerUIUrl() throws Exception {
		assertThat(ambariService.getResourceManagerUIUrl(), is("centralregion02.cloud.hortonworks.com:8088"));
	}	
	
	@Test
	public void getOozieServerUrl() throws Exception {
		assertThat(ambariService.getOozieServerUrl(), is("http://centralregion03.cloud.hortonworks.com:11000/oozie"));
	}	
	
	@Test
	public void getKafkaBrokerList() throws Exception  {
		List<String> kafkaHosts = ambariService.getKafkaBrokerList();
		assertThat(kafkaHosts.size(), is(2));
		assertTrue(kafkaHosts.contains("centralregion01.cloud.hortonworks.com"));
		assertTrue(kafkaHosts.contains("centralregion02.cloud.hortonworks.com"));
	}
	
	@Test
	public void getKafkaBrokerPort() throws Exception {
		assertThat(ambariService.getKafkaBrokerPort(), is("6667"));
	}
	
	@Test
	public void getKafkaZookeeperConnect() throws Exception  {
		assertThat(ambariService.getKafkaZookeeperConnect(), is("centralregion01.cloud.hortonworks.com:2181,centralregion02.cloud.hortonworks.com:2181,centralregion03.cloud.hortonworks.com:2181"));
	}	

	@Test
	public void collectBluePrint() {
		String fileToStoreBluePrint = null;
		String bluePrintContent = ambariService.collectClusterBluePrint(fileToStoreBluePrint);
		LOG.info(bluePrintContent);
		
	}

}
