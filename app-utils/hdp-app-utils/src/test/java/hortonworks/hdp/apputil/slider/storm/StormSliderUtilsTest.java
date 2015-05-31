package hortonworks.hdp.apputil.slider.storm;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtils;

import org.junit.Test;


public class StormSliderUtilsTest {

	

	
	public static final String SLIDER_STORM_PUBLISHER_URL = "http://centralregion09.cloud.hortonworks.com:56232/ws/v1/slider/publisher";

	
	private StormSliderUtils sliderService = new StormSliderUtils(SLIDER_STORM_PUBLISHER_URL);

	
	@Test
	public void getStormZookeeperQuorum() throws Exception{
		assertThat(sliderService.getStormZookeeperQuorum(), is("['centralregion01.cloud.hortonworks.com,centralregion02.cloud.hortonworks.com,centralregion03.cloud.hortonworks.com']"));
	}
	
	@Test
	public void getStormNimbusPort() throws Exception{
		assertThat(sliderService.getStormNimbusPort(), is("46464"));
	}	
	
	@Test
	public void getStormNimbusHost() throws Exception{
		assertThat(sliderService.getStormNimbusHost(), is("centralregion08.cloud.hortonworks.com"));
	}	
	
	@Test
	public void getStormUIServerHost() throws Exception {
		assertThat(sliderService.getStormUIServer(), is("centralregion09.cloud.hortonworks.com"));
	}
	
	@Test
	public void getStormUIPort() throws Exception {
		assertThat(sliderService.getStormUIPort(), is("57725"));
	}	


}

