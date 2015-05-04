package hortonworks.hdp.refapp.ecm;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.registry.ECMRegistryKeys;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class RegistryTest extends BaseTest {

	
	private static final Logger LOG = Logger.getLogger(RegistryTest.class);
	
	@Autowired
	HDPServiceRegistry serviceRegistry;
	
	@Test
	public void createRegistry() throws Exception {
		assertNotNull(serviceRegistry.getHBaseZookeeperClientPort());
		assertNotNull((serviceRegistry.getHBaseZookeeperHost()));
		assertNotNull(serviceRegistry.getHBaseZookeeperZNodeParent());
		assertThat(serviceRegistry.getCustomValue(ECMRegistryKeys.SOLR_SERVER_URL), is("http://vett-search01.cloud.hortonworks.com:8983/solr"));
		assertThat(serviceRegistry.getCustomValue(ECMRegistryKeys.ECM_SOLR_CORE), is("rawdocs"));
		assertThat(serviceRegistry.getAmbariServerUrl(), is("http://centralregion01.cloud.hortonworks.com:8080"));
		assertThat(serviceRegistry.getClusterName(), is("centralregioncluster"));
	
	}
}
