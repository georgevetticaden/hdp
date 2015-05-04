package hortonworks.hdp.refapp.ecm.registry;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.service.core.docstore.DocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.docstore.HBaseDocumentStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.IndexStore;
import hortonworks.hdp.refapp.ecm.service.core.indexstore.SolrIndexStore;

import org.springframework.context.ApplicationContext;

/**
 * Refreshes the appropriate ECM Beans when the service Registry configuration changes.
 * @author gvetticaden
 *
 */
public class ECMBeanRefresher {
	
	private HDPServiceRegistry registry;
	private ApplicationContext appContext;

	public ECMBeanRefresher(HDPServiceRegistry updatedRegistry, ApplicationContext context) {
		this.registry = updatedRegistry;
		this.appContext = context;
	}
	
	public void refreshBeans() {
		refreshDocumentStoreInAppContext();
		refreshIndexStoreInAppContext();
		
	}
	
	private void refreshDocumentStoreInAppContext() {
		HBaseDocumentStore docStore = (HBaseDocumentStore) appContext.getBean(DocumentStore.class);
		docStore.initialize();
		
	}

	private void refreshIndexStoreInAppContext() {
		SolrIndexStore indexStore = (SolrIndexStore) appContext.getBean(IndexStore.class);
		indexStore.initialize();
		
	}	

}
