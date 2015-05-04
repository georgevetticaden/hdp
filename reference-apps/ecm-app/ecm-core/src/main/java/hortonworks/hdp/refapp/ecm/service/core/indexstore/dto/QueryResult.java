package hortonworks.hdp.refapp.ecm.service.core.indexstore.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class QueryResult implements Serializable {

	private Long numFound;
	
	private List<Map<String, String>> results;
	
	private Long start;
	
	private Map<String, Map<String, Long>> facets;
	
	private Map<String, List<List<String>>> highlights;

	
	
	public QueryResult(Long numFound, List<Map<String, String>> results,
			Long start, Map<String, Map<String, Long>> facets,
			Map<String, List<List<String>>> highlights) {
		super();
		this.numFound = numFound;
		this.results = results;
		this.start = start;
		this.facets = facets;
		this.highlights = highlights;
	}

	public Long getNumFound() {
		return numFound;
	}

	public void setNumFound(Long numFound) {
		this.numFound = numFound;
	}

	public List<Map<String, String>> getResults() {
		return results;
	}

	public void setResults(List<Map<String, String>> results) {
		this.results = results;
	}

	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}



	public Map<String, Map<String, Long>> getFacets() {
		return facets;
	}

	public void setFacets(Map<String, Map<String, Long>> facets) {
		this.facets = facets;
	}

	public Map<String, List<List<String>>> getHighlights() {
		return highlights;
	}

	public void setHighlights(Map<String, List<List<String>>> highlights) {
		this.highlights = highlights;
	}
	
	
	
}
