package hortonworks.hdp.refapp.trucking.storm.bolt.solr;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;



import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class SolrIndexingBolt implements IRichBolt {


	private static final long serialVersionUID = -5319490672681173657L;
	private static final Logger LOG = LoggerFactory.getLogger(SolrIndexingBolt.class);
	
	private OutputCollector collector;
	private Properties config;

	private SolrServer server = null;	
	private String solrUrl;	

	
	public SolrIndexingBolt(Properties config) {
		this.config = config;

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.solrUrl = config.getProperty("solr.server.url") + "/" + config.getProperty("trucking.solr.core");
		server = new HttpSolrServer(solrUrl);
		(new Thread(new CommitThread(server))).start();		
	}

	@Override
	public void execute(Tuple input) {
		TruckEvent event = constructTruckEvent(input);
		index(event);
		collector.ack(input);
	}



	private void index(TruckEvent truckEvent) {
		try {
			LOG.info("Starting process to solr index document["+truckEvent+"]");
			UpdateResponse response = server.addBean(truckEvent);
			LOG.info("Indexed document with id: " + truckEvent.getId()
					+ " status: " + response.getStatus());
		} catch (IOException e) {
			LOG.error("Could not index document: " + truckEvent.getId()
					+ " " + e.getMessage());
			e.printStackTrace();
		} catch (SolrServerException e) {
			LOG.error("Could not index document: " + truckEvent.getId()
					+ " " + e.getMessage());
			e.printStackTrace();
		}
	}

	private TruckEvent constructTruckEvent(Tuple input) {
		String eventKey = input.getStringByField("eventKey");
		int driverId = input.getIntegerByField("driverId");
		int truckId = input.getIntegerByField("truckId");
		Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
		long eventTimeLong = eventTime.getTime();
		Date time = new Date(eventTimeLong);			
		String eventType = input.getStringByField("eventType");
		double longitude = input.getDoubleByField("longitude");
		double latitude = input.getDoubleByField("latitude");
		String driverName = input.getStringByField("driverName");
		int routeId = input.getIntegerByField("routeId");
		String routeName = input.getStringByField("routeName");	
		
		TruckEvent event = new TruckEvent(eventKey, driverId, truckId, time, eventType, longitude, latitude, driverName, routeId, routeName);
		return event;
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
	

	

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}	
	
	class CommitThread implements Runnable {
		SolrServer server;

		public CommitThread(SolrServer server) {
			this.server = server;
		}

		public void run() {
			while (true) {
				try {
					Thread.sleep(15000);
					server.commit();
					LOG.info("Committing Index");
				} catch (InterruptedException e) {
					LOG.error("Interrupted: " + e.getMessage());
					e.printStackTrace();
				} catch (SolrServerException e) {
					LOG.error("Error committing: " + e.getMessage());
					e.printStackTrace();
				} catch (IOException e) {
					LOG.error("Error committing: " + e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}	

}
