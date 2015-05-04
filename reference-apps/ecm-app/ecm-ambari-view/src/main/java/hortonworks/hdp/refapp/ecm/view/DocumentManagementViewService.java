package hortonworks.hdp.refapp.ecm.view;

import hortonworks.hdp.apputil.registry.DeploymentMode;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.apputil.registry.ServiceRegistryParams;
import hortonworks.hdp.refapp.ecm.config.ECMViewHDPServiceRegistryConfig;
import hortonworks.hdp.refapp.ecm.registry.ECMBeanRefresher;
import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;
import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.api.DocumentServiceAPI;
import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;
import hortonworks.hdp.refapp.ecm.service.config.DocumentServiceConfig;
import hortonworks.hdp.refapp.ecm.service.config.DocumentStoreConfig;
import hortonworks.hdp.refapp.ecm.service.config.IndexStoreConfig;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.DocumentSearchRequest;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.dto.QueryResult;
import hortonworks.hdp.refapp.ecm.util.MimeUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.ambari.view.ViewContext;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.solr.repository.support.SolrRepositoryFactory;

import com.google.inject.Inject;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataParam;

/**
 * Rest Endpoints for the ECM Ambari View
 * @author gvetticaden
 *
 */
//@Path("ecm")
public class DocumentManagementViewService {

	private static final Logger LOG = Logger.getLogger(DocumentManagementViewService.class);

	@Inject
	protected ViewContext context;

	private DocumentServiceAPI docServiceAPI;
	private ApplicationContext appContext;



	  @GET
	  @Produces({"text/html"})
	  public Response getUsage(@Context HttpHeaders headers, @Context UriInfo ui) throws IOException{
		  SolrRepositoryFactory factory = null;
	    String entity = "<h2>Usage of ECM Service</h2><br>" +
	        "<ul>" +
	        "<li>/search/{query}/{filter:.*}/{start:.*}/{facet:.*}/{highlight:.*}</li>" +
	        "</ul>"
	        ;
	    return Response.ok(entity).type("text/html").build();
	  }	


	/**
	 * Searches for Documents in the IndexStore
	 * @param searchValue
	 * @param filter
	 * @param start
	 * @param facet
	 * @param highlight
	 * @return
	 * @throws Exception
	 */
	@GET
	@Path("/search/{query}/{filter:.*}/{start:.*}/{facet:.*}/{highlight:.*}")
	@Produces({ "application/json" })
	public QueryResult search(@PathParam("query") String searchValue,
			@PathParam("filter") String filter,
			@PathParam("start") Integer start,
			@PathParam("facet") String facet,
			@PathParam("highlight") String highlight) throws Exception {

		LOG.info("Query String is: " + searchValue);
		LOG.info("Filter is: " + filter);
		LOG.info("Start Value: " + start);
		LOG.info("Facet value: " + facet);
		LOG.info("highlight value is: " + highlight);

		DocumentSearchRequest request = new DocumentSearchRequest();
		request.setSearchValue(searchValue);
		request.setStart(start == null ? 0 : start);
		request.setFacetField(facet);
		request.setHighlightField(highlight);

		QueryResult result = null;
		try {
			LOG.info("Executing Search with Searh Value[" + searchValue + "]");
			result = getDocumentService().searchDocuments(request);
			LOG.info("Finished Search with Searh Value[" + searchValue
					+ "]. Result Size is: " + result.getNumFound());
		} catch (Exception e) {
			LOG.error("Error searching", e);
		}
		return result;
	}
	
	/**
	 * Uplaods Document to the Document and INdex Store
	 * @param documentName
	 * @param documentClass
	 * @param customerName
	 * @param fileInputStream
	 * @param body
	 * @return
	 * @throws Exception
	 */
	@POST
	@Path("/upload")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	@Produces(MediaType.TEXT_PLAIN)
	public String uploadDocumentAndMetadata(
			@FormDataParam ("documentName") String documentName, 
			@FormDataParam ("documentClass") String documentClass, 
			@FormDataParam ("customerName")String customerName, 
			@FormDataParam("file") InputStream fileInputStream,
			@FormDataParam("file") FormDataBodyPart body ) throws Exception {
		
		if(fileInputStream == null) {
			String errMsg = "File Stream is null";
			LOG.error(errMsg);
			throw new RuntimeException(errMsg);
		}
		
		byte[] byteArray = IOUtils.toByteArray(fileInputStream);	
		String mimeType = body.getMediaType().getType() + "/" + body.getMediaType().getSubtype();
		
		if(LOG.isInfoEnabled()) {
			LOG.info("Uploading Doc....[" + documentName + "]");
			LOG.info("DocumentName: " + documentName);
			LOG.info("DocumentClass: " + documentClass);
			LOG.info("Customer Name: " + customerName);
			LOG.info("File Bytes from MultiPart: " + byteArray);
			LOG.info("File Name from MultiPart: " + body.getContentDisposition().getFileName());
			LOG.info("File Type from MultiPart: " + mimeType);
			
		}
		

		String extension = MimeUtils.getExtension(mimeType);
		DocumentMetaData docMetadata = new DocumentMetaData(documentName, mimeType, DocumentClass.valueOf(documentClass), customerName, extension);
		DocumentUpdateRequest docUpdateRequest = new DocumentUpdateRequest(null, byteArray, docMetadata);
		getDocumentService().addDocument(docUpdateRequest);
		return "Uploaded Successfully";
	}  

	private DocumentServiceAPI getDocumentService() {
		if (this.docServiceAPI == null) {
			try {
				initialize();
			} catch (Exception e) {
				String errMsg = "Error Initializing apppContext";
				throw new RuntimeException(errMsg, e);
			}
		}
		return this.docServiceAPI;
	}

	private ApplicationContext createAppContext(ViewContext viewContext)
			throws Exception {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.setClassLoader(DocumentManagementViewService.class.getClassLoader());
		context.register(DocumentServiceConfig.class,  IndexStoreConfig.class, DocumentStoreConfig.class,
				ECMViewHDPServiceRegistryConfig.class);
		context.refresh();

		Map<String, String> ambariViewProps = viewContext.getProperties();
		LOG.info("Ambari View properties used to populate the registry are: "
				+ ambariViewProps);

		HDPServiceRegistry serviceRegistry = context
				.getBean(HDPServiceRegistry.class);
		
		ServiceRegistryParams registryParams = createServiceRegistryParams(ambariViewProps);
		
		serviceRegistry.populate(registryParams, ambariViewProps, null);

		ECMBeanRefresher refresher = new ECMBeanRefresher(serviceRegistry,
				context);
		refresher.refreshBeans();
		return context;
	}

	private ServiceRegistryParams createServiceRegistryParams(
			Map<String, String> ambariViewProps) {
		ServiceRegistryParams registryParams = new ServiceRegistryParams();
		registryParams.setAmbariUrl(ambariViewProps.get(RegistryKeys.AMBARI_SERVER_URL));
		registryParams.setClusterName(ambariViewProps.get(RegistryKeys.AMBARI_CLUSTER_NAME));
		String hbaseDeployMode = ambariViewProps.get(RegistryKeys.HBASE_DEPLOYMENT_MODE);
		if(StringUtils.isNotEmpty(hbaseDeployMode)) {
			registryParams.setHbaseDeploymentMode(DeploymentMode.valueOf(hbaseDeployMode));
			registryParams.setHbaseSliderPublisherUrl(ambariViewProps.get(RegistryKeys.HBASE_SLIDER_PUBLISHER_URL));
		}
		
		String stormDeployMode = ambariViewProps.get(RegistryKeys.STORM_DEPLOYMENT_MODE);
		if(StringUtils.isNotEmpty(stormDeployMode)) {
			registryParams.setStormDeploymentMode(DeploymentMode.valueOf(stormDeployMode));
			registryParams.setStormSliderPublisherUrl(ambariViewProps.get(RegistryKeys.STORM_SLIDER_PUBLISHER_URL));
		}	
		return registryParams;
	}

	public void setContext(ViewContext context) {
		this.context = context;
	}
	
	private void initialize() throws Exception {
		configureViewContext();
		this.appContext = createAppContext(context);
		docServiceAPI = appContext.getBean(DocumentServiceAPI.class);
	}
	
	/*
	 * By Default the viewContext should be injected via ambari. 
	 * subclasses can mock it out for testing purposes
	 */
	protected void configureViewContext() {
		// TODO Auto-generated method stub
		
	}	

}
