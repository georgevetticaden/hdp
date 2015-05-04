package hortonworks.hdp.refapp.ecm.ingestion.utils;

import hortonworks.hdp.refapp.ecm.ingestion.flume.Constants;
import hortonworks.hdp.refapp.ecm.util.MimeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public final class DocUtils {

	private static final Logger LOG = Logger.getLogger(DocUtils.class);
	
	public static Map<String, String> constructDocMetadataFromFullFileName(String fullFileName) {
		Map<String, String> docMeta = new HashMap<String, String>();
		try {
			String[] fileParts = fullFileName.split(Pattern.quote("/"));
			if(fileParts.length > 0) {
				String fileNameWithExtension = fileParts[fileParts.length - 1];
				LOG.info(fileNameWithExtension);
				
				String[] fileNameParts = fileNameWithExtension.split(Pattern.quote("."));
				LOG.info(fileNameParts.length);
				
				String fileName = null;
				String extension = null;
				if(fileNameParts.length == 2) {
					fileName = fileNameParts[0];
					extension = fileNameParts[1];
				} else if(fileNameParts.length > 2) {
					int index = fileNameWithExtension.lastIndexOf(".");
					fileName= fileNameWithExtension.substring(0, index);
					extension = fileNameWithExtension.substring(index+1);
				}
				
				
				docMeta.put(Constants.HEADER_KEY_DOC_NAME, fileName);
				docMeta.put(Constants.HEADER_KEY_DOC_EXTENSION, extension);
				docMeta.put(Constants.HEADER_KEY_MIME_TYPE, lookupMimeType(extension));
				
				//TODO: Remove hardcoding of Doc Type and Custom Name when we figure how to to get it.
				docMeta.put(Constants.HEADER_KEY_DOC_CLASS_TYPE, "RFP");
				docMeta.put(Constants.HEADER_KEY_DOC_CUST_NAME, null);			
				
			} else {
				String errMsg = "File["+ fullFileName + "] is not in correct format";
				LOG.error(errMsg);
			}
			
		} catch (Exception e) {
			String errMsg = "Error when parsing File["+fullFileName + "] to create Metadata";
			LOG.error(errMsg, e);
		}
		return docMeta;
	}

	private static String lookupMimeType(String docExtension) {
		String mimeType = null;
		try {
			return MimeUtils.getMimeType(docExtension);
		} catch (Exception e) {
			String errMsg = "Doc Extension["+ docExtension + "] not supported. Returning null for mimeType";
			LOG.error(errMsg);
		}
		return mimeType;
	}	
}
