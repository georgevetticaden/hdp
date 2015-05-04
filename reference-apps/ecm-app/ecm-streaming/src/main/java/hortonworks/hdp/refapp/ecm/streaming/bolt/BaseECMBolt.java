package hortonworks.hdp.refapp.ecm.streaming.bolt;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.ecm.config.ECMStreamingHDPServiceRegistryConfig;
import hortonworks.hdp.refapp.ecm.registry.ECMBeanRefresher;
import hortonworks.hdp.refapp.ecm.service.config.DocumentStoreConfig;
import hortonworks.hdp.refapp.ecm.service.config.IndexStoreConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class BaseECMBolt implements IRichBolt {
	


	private static final Logger LOG = Logger.getLogger(BaseECMBolt.class);
	
	private Properties config;
	protected OutputCollector collector;
	

	public BaseECMBolt(Properties config) {
		super();
		this.config = config;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	protected void populateServiceRegistry(HDPServiceRegistry registry) {
		Map<String, String> paramsMap = null;
		try {
			paramsMap = createRegistryParamsMap();
			LOG.info("The Params Map used to popualte the registry is: " + paramsMap);
			registry.populate(null, paramsMap, null);
		} catch (Exception e) {
			String errMsg = "Error Populating Service Registry from params: " + paramsMap;
			LOG.error(errMsg, e);
			throw new RuntimeException(errMsg);
		}
	}
	
	protected ApplicationContext createAppContext() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(IndexStoreConfig.class, DocumentStoreConfig.class, ECMStreamingHDPServiceRegistryConfig.class);
		context.refresh();
		
		populateServiceRegistry(context.getBean(HDPServiceRegistry.class));
		
		ECMBeanRefresher refresher = new ECMBeanRefresher(context.getBean(HDPServiceRegistry.class), context);		
		refresher.refreshBeans();		
		return context;
	}	

	private Map<String, String> createRegistryParamsMap() {
		Map<String, String> paramsMap = new HashMap<String, String>();
		for(Object key: config.keySet()) {
			String keyString = (String) key;
			String value = (String) config.get(keyString);
			paramsMap.put(keyString, value);
		}
		return paramsMap;
	}	
	
	

}
