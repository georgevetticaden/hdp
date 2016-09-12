package hortonworks.hdp.refapp.trucking.storm.bolt.alert.window.rule;


import hortonworks.hdp.refapp.trucking.domain.TruckDriver;
import hortonworks.hdp.refapp.trucking.domain.TruckDriverInfractionDetail;

import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Bolt that executes various trucking alert rules using the Infraction Rules Engine
 * @author gvetticaden
 *
 */
public class InfractionRulesBolt implements IRichBolt {
	

	private static final long serialVersionUID = 6816706717943954742L;
	private static final Logger LOG = LoggerFactory.getLogger(InfractionRulesBolt.class);
	
	private Properties config;	
	private OutputCollector collector;
	private InfractionRulesEngine infractionRulesEngine;

	public InfractionRulesBolt(Properties config) {
		this.config = config;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.infractionRulesEngine = new InfractionRulesEngine(config);
	}

	@Override
	public void execute(Tuple input) {
		Map<TruckDriver, TruckDriverInfractionDetail> truckDriverInfractions = getDriverInfractions(input);
		
		LOG.debug("The number drivers with infractions is: " + truckDriverInfractions.size());
		LOG.debug("The infractions being processed by rules bolt are: " + truckDriverInfractions);
		
		for(TruckDriver truckDriver: truckDriverInfractions.keySet()) {
			TruckDriverInfractionDetail infractionDetail = truckDriverInfractions.get(truckDriver);
			infractionRulesEngine.processEvent(infractionDetail);
		}
		collector.ack(input);
	}

	private Map<TruckDriver, TruckDriverInfractionDetail> getDriverInfractions(Tuple input) {
		Map<TruckDriver, TruckDriverInfractionDetail> truckDriverInfractions = (Map<TruckDriver, TruckDriverInfractionDetail> )input.getValueByField("driversInfractionDetails");
		return truckDriverInfractions;
	}

	@Override
	public void cleanup() {
		infractionRulesEngine.cleanUpResources();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
