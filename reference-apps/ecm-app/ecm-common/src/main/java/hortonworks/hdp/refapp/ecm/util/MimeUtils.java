package hortonworks.hdp.refapp.ecm.util;

import java.util.HashMap;
import org.apache.log4j.Logger;

public final class MimeUtils {
	
	private static final Logger LOG = Logger.getLogger(MimeUtils.class);
	
	// Map of extension --> mimeType
	private static final HashMap<String, String> mimeMap = new HashMap<String, String>();
	
	static {
		mimeMap.put("pdf", "application/pdf");
		mimeMap.put("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
		mimeMap.put("doc", "application/msword");
	}

	
	public static final String getExtension(String mimeTypeRequest) {
		for(String extension: mimeMap.keySet()) {
			
			String mimeType = mimeMap.get(extension);
			if(mimeType.equals(mimeTypeRequest)) {
				return extension;
			}
			
		}
		String errMsg = "Unrecognized mime-type["+ mimeTypeRequest + "] to get a corresponding extension";
		LOG.error(errMsg);
		throw new RuntimeException(errMsg);
	}
	
	public static final String getMimeType(String extensionRequest) {
		for(String extension: mimeMap.keySet()) {
			
			if(extension.equals(extensionRequest)) {
				return mimeMap.get(extension);
			}
			
		}
		String errMsg = "Unrecognized extension["+ extensionRequest + "] to get a corresponding mime Type";
		LOG.error(errMsg);
		throw new RuntimeException(errMsg);
	}	
		
}
