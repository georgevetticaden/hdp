package hortonworks.hdp.apputil.ambari;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

public class AmbariUtilsTest {
	
	private static final String AMBARI_URL = "http://hdp0.field.hortonworks.com:8080/api/v1/clusters/HDP_2_5";
	//private static final String AMBARI_URL = "http://10.22.0.210:8080/api/v1/clusters/opensoc";
	
	private static final Logger LOG = LoggerFactory.getLogger(AmbariUtils.class);
	AmbariUtils ambariService = new AmbariUtils(AMBARI_URL); 
		

	@Test
	public void getHBaseZookeeperQuorum() throws Exception {
		List<String> quorumList = ambariService.getHBaseZookeeperQuorum();
		assertThat(quorumList.size(), is(3));
		assertTrue(quorumList.contains("hdp0.field.hortonworks.com"));
		assertTrue(quorumList.contains("hdp1.field.hortonworks.com"));
		assertTrue(quorumList.contains("hdp2.field.hortonworks.com"));
			
	}
	
	@Test
	public void getHBaseZookeeperCientPort() throws Exception {
		assertThat(ambariService.getHBaseZookepperCientPort(), is("2181"));
	}
	
	@Test
	public void getHBaseZookeeperParentNode() throws Exception {
		assertThat(ambariService.getHBaseZookeeperParentNode(), is("/hbase-unsecure"));
	}	
	
	@Test
	public void getStormZookeeperQuorum() throws Exception{
		assertThat(ambariService.getStormZookeeperQuorum(), is("['hdp0.field.hortonworks.com','hdp1.field.hortonworks.com','hdp2.field.hortonworks.com']"));
	}
	
	@Test
	public void getStormNimbusPort() throws Exception{
		assertThat(ambariService.getStormNimbusPort(), is("6627"));
	}	
	
	@Test
	public void getStormNimbusHost() throws Exception{
		assertThat(ambariService.getStormNimbusHostList().get(0), is("hdp1.field.hortonworks.com"));
	}	
	
	@Test
	public void getStormUIServerHost() throws Exception {
		assertThat(ambariService.getStormUIServer(), is("hdp1.field.hortonworks.com"));
	}
	
	@Test
	public void getStormUIPort() throws Exception {
		assertThat(ambariService.getStormUIPort(), is("8744"));
	}	

	@Test
	public void getHDFSUrl() throws Exception{
		assertThat(ambariService.getHDFSUrl(), is("hdfs://hdp0.field.hortonworks.com:8020"));
	}	

	@Test
	public void getHiveMetaStoreUrl() throws Exception{
		assertThat(ambariService.getHiveMetaStoreUrl(), is("thrift://hdp2.field.hortonworks.com:9083"));
	}	
	
	@Test
	public void getHiveServer2Host() throws Exception {
		assertThat(ambariService.getHiveServer2Host(), is("hdp2.field.hortonworks.com"));
	}	
	
	@Test
	public void getHiveServer2ThriftPort() throws Exception {
		assertThat(ambariService.getHiveServer2ThriftPort(), is("10000"));
	}		
	
	
	
	@Test
	public void getResourceManagerURL() throws Exception {
		assertThat(ambariService.getResourceManagerUrl(), is("hdp1.field.hortonworks.com:8050"));
	}
	
	@Test
	public void getResourceManagerUIUrl() throws Exception {
		assertThat(ambariService.getResourceManagerUIUrl(), is("hdp1.field.hortonworks.com:8088"));
	}	
	
	@Test
	public void getOozieServerUrl() throws Exception {
		assertThat(ambariService.getOozieServerUrl(), is("http://hdp2.field.hortonworks.com:11000/oozie"));
	}	
	
	@Test
	public void getKafkaBrokerList() throws Exception  {
		List<String> kafkaHosts = ambariService.getKafkaBrokerList();
		LOG.info("Kafka Host", kafkaHosts);
		assertThat(kafkaHosts.size(), is(1));
		assertTrue(kafkaHosts.contains("hdp0.field.hortonworks.com"));
	}
	
	@Test
	public void getKafkaBrokerPort() throws Exception {
		assertThat(ambariService.getKafkaBrokerPort(), is("6667"));
	}
	
	@Test
	public void getKafkaZookeeperConnect() throws Exception  {
		assertThat(ambariService.getKafkaZookeeperConnect(), is("hdp0.field.hortonworks.com:2181,hdp1.field.hortonworks.com:2181,hdp2.field.hortonworks.com:2181"));
	}	

	@Test
	public void collectBluePrint() {
		String fileToStoreBluePrint = null;
		String bluePrintContent = ambariService.collectClusterBluePrint(fileToStoreBluePrint);
		LOG.info(bluePrintContent);
		
	}

}
