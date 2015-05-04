package hortonworks.hdp.refapp.ecm.view;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Application;

import org.apache.log4j.Logger;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DocmentManagementViewServiceTest extends JerseyTest {
	
	private static final Logger LOG = Logger.getLogger(DocmentManagementViewServiceTest.class);

	private ObjectMapper mapper = new ObjectMapper();
	
    @Override
    protected Application configure() {
        return new ResourceConfig(DocumentManagementViewServiceWithDummyContext.class);
    }	
    

	 @Test
    public void testUsage() {
		
    	String url = "/ecm";
    	final String result = target(url).request().get(String.class);
    	LOG.info("Usage instructions are: " + result);
    }

	@Test
	public void testSearch() throws Exception {
		String searchString = "knox";
		String filter = "";
		String start = null;
		String facet = "last_author";
		String hightlight = "body";
		
		String url = constructURL(searchString, filter, start, facet, hightlight);
		LOG.info("Url is: " + url);
		
		final String result = target(url).request().get(String.class);
		
		Map resultMap = mapper.readValue(result, Map.class);
		assertNotNull(resultMap);
		assertNotNull(resultMap.get("numFound"));
		int resultSize = (Integer)resultMap.get("numFound");
		assertThat(resultSize, is(1));
		
		assertNotNull(resultMap.get("results"));
		List<Object> results = (List<Object>)resultMap.get("results");
		assertNotNull(results);
		assertThat(results.size(), is(1));
		Map resultValueMap = (Map)results.get(0);
		
		assertNotNull(resultValueMap.get("id"));
		assertNotNull(resultValueMap.get("body"));
		
		assertNotNull(resultMap.get("facets"));
		Map facetsMap = (Map)resultMap.get("facets");
		Map lastAuthorFacetMap =  (Map)facetsMap.get("last_author");
		Set keys = lastAuthorFacetMap.keySet();
		assertThat(keys.size(), is(1));
		assertThat((String)keys.iterator().next(), is("Phil Zacharia"));
		//assertThat(value, is("Phil"));
		
		
		
	}

	private String constructURL(String searchString, String filter, String start,
			String facet, String hightlight) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("/ecm/search/");
		buffer.append(searchString == null ? "" : searchString).append("/");
		buffer.append(filter == null ? "" : filter).append("/");
		buffer.append(start == null ? "": start).append("/");
		buffer.append(facet == null ? "": facet).append("/");
		buffer.append(hightlight == null ? "" : hightlight);
		return buffer.toString();
	}   
          
	
}
