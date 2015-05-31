package hortonworks.hdp.apputil.slider.hbase;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtils;

import java.util.List;

import org.junit.Test;


public class HBaseSliderUtilsTest{

	
	public static final String SLIDER_HBASE_PUBLISHER_URL = "http://centralregion09.cloud.hortonworks.com:34112/ws/v1/slider/publisher";
	
	private HBaseSliderUtils sliderService = new HBaseSliderUtils(SLIDER_HBASE_PUBLISHER_URL);

	@Test
	public void getHBaseZookepperCientPort() throws Exception {
		
		assertThat(sliderService.getHBaseZookepperCientPort(), is("2181"));
	}
	
	@Test
	public void getHBaseZookeeperParentNode() throws Exception {
		assertThat(sliderService.getHBaseZookeeperParentNode(), is("/services/slider/users/yarn/hbase-on-yarn-v36"));
	}
	
	@Test
	public void getHBaseZookeeperQuorum() throws Exception {
		List<String> quorumList = sliderService.getHBaseZookeeperQuorum();
		assertThat(quorumList.size(), is(3));
		assertTrue(quorumList.contains("centralregion01.cloud.hortonworks.com"));
		assertTrue(quorumList.contains("centralregion02.cloud.hortonworks.com"));
		assertTrue(quorumList.contains("centralregion03.cloud.hortonworks.com"));
	}		
	

}

