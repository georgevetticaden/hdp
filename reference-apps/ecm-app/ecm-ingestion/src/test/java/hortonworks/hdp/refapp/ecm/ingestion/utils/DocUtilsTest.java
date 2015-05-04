package hortonworks.hdp.refapp.ecm.ingestion.utils;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import hortonworks.hdp.refapp.ecm.ingestion.flume.Constants;

import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Test;

public class DocUtilsTest {
	

	@Test
	public void testConstructDocMetadataFromFullFileName() {
		String testFullFileName = "/mnt/flume-spool-dir/rfi/AmFam_Enterprise Wide Hadoop Cluster RFP.doc";
		Map<String, String> docMeta = DocUtils.constructDocMetadataFromFullFileName(testFullFileName);
		
		assertNotNull(docMeta);
		assertThat(docMeta.size(), is(5));
		
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_CLASS_TYPE), is("RFP"));
		assertNull(docMeta.get(Constants.HEADER_KEY_DOC_CUST_NAME));
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_EXTENSION), is("doc"));
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_NAME), is("AmFam_Enterprise Wide Hadoop Cluster RFP"));
		assertThat(docMeta.get(Constants.HEADER_KEY_MIME_TYPE), is("application/msword"));
		
	}
	
	@Test
	public void testConstructDocMetadataFromFullFileNameForFileWithPeriods() {
		String testFullFileName = "/mnt/flume-spool-dir/rfi/JPMC Hortonworks RFI 5.8.13Final.docx";
		Map<String, String> docMeta = DocUtils.constructDocMetadataFromFullFileName(testFullFileName);
		
		assertNotNull(docMeta);
		assertThat(docMeta.size(), is(5));
		
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_CLASS_TYPE), is("RFP"));
		assertNull(docMeta.get(Constants.HEADER_KEY_DOC_CUST_NAME));
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_EXTENSION), is("docx"));
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_NAME), is("JPMC Hortonworks RFI 5.8.13Final"));
		assertThat(docMeta.get(Constants.HEADER_KEY_MIME_TYPE), is("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
		
	}	
	
	//
	@Test
	public void test3() {
		String testFullFileName = "/mnt/flume-spool-dir/rfi/1.0.1_(PA)_B-Technical_Questionnaire_NVE_Data_Analytics_Platform_FINAL.docx";
		Map<String, String> docMeta = DocUtils.constructDocMetadataFromFullFileName(testFullFileName);
		
		assertNotNull(docMeta);
		assertThat(docMeta.size(), is(5));
		
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_CLASS_TYPE), is("RFP"));
		assertNull(docMeta.get(Constants.HEADER_KEY_DOC_CUST_NAME));
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_EXTENSION), is("docx"));
		assertThat(docMeta.get(Constants.HEADER_KEY_DOC_NAME), is("JPMC Hortonworks RFI 5.8.13Final"));
		assertThat(docMeta.get(Constants.HEADER_KEY_MIME_TYPE), is("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
		
	}		
	
	
	@Test
	public void test() {
		String x = "AmFam_Enterprise Wide Hadoop Cluster RFP.doc";
		String[] xSplit = x.split(Pattern.quote("."));
		System.out.println(xSplit.length);
	}
}
