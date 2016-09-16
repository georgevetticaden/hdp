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
public class InfractionRulesV2Bolt implements IRichBolt {
	

	private static final long serialVersionUID = 6816706717943954742L;
	private static final Logger LOG = LoggerFactory.getLogger(InfractionRulesV2Bolt.class);
	
	private Properties config;	
	private OutputCollector collector;
	private InfractionRulesEngine infractionRulesEngine;

	public InfractionRulesV2Bolt(Properties config) {
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
		TruckDriverInfractionDetail truckDriverInfractions = getDriverInfractions(input);
		
		infractionRulesEngine.processEvent(truckDriverInfractions);
		
		collector.ack(input);
	}

	private TruckDriverInfractionDetail getDriverInfractions(Tuple input) {
		TruckDriverInfractionDetail truckDriverInfractions = (TruckDriverInfractionDetail)input.getValueByField("truckDriverInfractionDetail");
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
