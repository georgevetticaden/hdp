package hortonworks.hdp.refapp.ecm.service.core.indexstore.dto;

public class DocumentSearchRequest {
	
	private String searchValue;
	private String filter;
	private Integer start;
	private String facetField;
	private String highlightField;
	
	public String getSearchValue() {
		return searchValue;
	}
	public void setSearchValue(String searchValue) {
		this.searchValue = searchValue;
	}
	public String getFilter() {
		return filter;
	}
	public void setFilter(String filter) {
		this.filter = filter;
	}
	public Integer getStart() {
		return start;
	}
	public void setStart(Integer start) {
		this.start = start;
	}
	public String getFacetField() {
		return facetField;
	}
	public void setFacetField(String facetField) {
		this.facetField = facetField;
	}
	public String getHighlightField() {
		return highlightField;
	}
	public void setHighlightField(String highlightField) {
		this.highlightField = highlightField;
	}
	
	
	
	
}
