package hortonworks.hdp.refapp.ecm.streaming.topology;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

public abstract class BaseDocumentTopology {

	
	private static final Logger LOG = Logger.getLogger(BaseDocumentTopology.class);

	protected Properties topologyConfig;
	
	public BaseDocumentTopology(String configFileLocation) throws Exception {
		
		topologyConfig = new Properties();
		try {
			topologyConfig.load(new FileInputStream(configFileLocation));
			System.out.println("Dumping out...... config file......");
			LOG.info(topologyConfig);
		} catch (FileNotFoundException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		} catch (IOException e) {
			LOG.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		}			
	}
	
	public BaseDocumentTopology() {
		
	}

	public Properties getTopologyConfig() {
		return topologyConfig;
	}
	
	
}
