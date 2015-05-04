package hortonworks.hdp.refapp.ecm.ingestion.flume.interceptor;

import hortonworks.hdp.refapp.ecm.ingestion.utils.DocUtils;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

public class DocumentInterceptor implements Interceptor {

	private static final String HEADER_FILE_NAME_KEY = "resourceName";
	private static final Logger LOG = Logger.getLogger(DocumentInterceptor.class);
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize() {
		
	}

	@Override
	public Event intercept(Event event) {
		Map<String, String> headers = event.getHeaders();
		if(headers != null) {
			String fullFileName = headers.get(HEADER_FILE_NAME_KEY);
			LOG.info("File["+ fullFileName + "] being parsed to extract metadata");
			
			Map<String, String> docMetaData = DocUtils.constructDocMetadataFromFullFileName(fullFileName);
			headers.putAll(docMetaData);
			
			LOG.info("After adding docMeta, the customer headers is: " + headers);
		} else {
			LOG.info("Headers passed in is null");
		}
		return event;
	}




	@Override
	public List<Event> intercept(List<Event> events) {
		for(Event event: events) {
			intercept(event);
		}
		return events;
	}
	
	public static class Builder implements Interceptor.Builder {

		private Context context;

		@Override
		public void configure(Context context) {
			this.context = context;
		
		}

		@Override
		public Interceptor build() {
			return new DocumentInterceptor();
		}
		
	}

}
