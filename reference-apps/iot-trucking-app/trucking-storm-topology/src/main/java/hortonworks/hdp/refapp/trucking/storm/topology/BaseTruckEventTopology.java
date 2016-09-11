package hortonworks.hdp.refapp.trucking.storm.topology;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseTruckEventTopology {

	
	private static final Logger LOG = LoggerFactory.getLogger(BaseTruckEventTopology.class);

	protected Properties topologyConfig;
	
	public BaseTruckEventTopology(String configFileLocation) throws Exception {
		
		topologyConfig = new Properties();
		try {
			topologyConfig.load(new FileInputStream(configFileLocation));
			System.out.println("Dumping out...... config file......");
			LOG.info("Topology  Config:", topologyConfig);
			
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
	
	public BaseTruckEventTopology() {
		
	}

	public Properties getTopologyConfig() {
		return topologyConfig;
	}
	
	
}
