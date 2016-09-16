package hortonworks.hdp.refapp.trucking.storm.bolt.join;

import hortonworks.hdp.refapp.trucking.domain.InfractionCount;
import hortonworks.hdp.refapp.trucking.domain.TruckDriver;
import hortonworks.hdp.refapp.trucking.domain.TruckDriverInfractionDetail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JoinTruckSpeedAndDriverInfractionBolt extends BaseWindowedBolt {


	private static final long serialVersionUID = 8257758116690028179L;
	private static final Logger LOG = LoggerFactory.getLogger(JoinTruckSpeedAndDriverInfractionBolt.class);
	
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }	
	
    

	@Override
	public void execute(TupleWindow inputWindow) {
		
		List<Tuple> tuplesInWindow = inputWindow.get();

		
		Map<TruckDriver, TruckDriverInfractionDetail> truckDriversInfractionDetails = new HashMap<TruckDriver, TruckDriverInfractionDetail>();
		Map<TruckDriver, Integer> truckDriversAverageSpeed = new HashMap<TruckDriver, Integer>();
		
		LOG.info("In Join for Infraction And Speed, window size is: " + tuplesInWindow.size());
		
		for(Tuple tuple: tuplesInWindow) {
			
		
			TruckDriver truckDriver = (TruckDriver) tuple.getValueByField("truckDriver");
			
			String sourceComponent = tuple.getSourceComponent();
			if("3-Min-Count-Window".equals(sourceComponent)) {	
				TruckDriverInfractionDetail truckDriverInfraction = (TruckDriverInfractionDetail) tuple.getValueByField("infractionDetail");
				truckDriversInfractionDetails.put(truckDriver, truckDriverInfraction);
				
				
			} else if("5-Min-Sliding-Avg-Speed".equals(sourceComponent)) {
				
				Integer averageSpeed = (Integer) tuple.getValueByField("averageSpeed");
				truckDriversAverageSpeed.put(truckDriver, averageSpeed);		
				
			} else {
				LOG.info("Got a Tuple["+tuple+"] from unidentified source");
			}
			collector.ack(tuple);
		}
		
		LOG.info("truckDriversInfractionDetails["+truckDriversInfractionDetails.size() + "] is going to be joined with truckDriversAverageSpeed["+truckDriversAverageSpeed.size() +"]");
		
		/* do a left outer join */
		joinAndEmit(truckDriversInfractionDetails,truckDriversAverageSpeed);
		
	
	}
	
	 private void joinAndEmit(
			Map<TruckDriver, TruckDriverInfractionDetail> truckDriversInfractionDetail,
			Map<TruckDriver, Integer> truckDriversAverageSpeed) {
		
		 for(TruckDriver truckDriver: truckDriversInfractionDetail.keySet()) {
			TruckDriverInfractionDetail infractionDetail = truckDriversInfractionDetail.get(truckDriver);
			Integer averageSpeed = truckDriversAverageSpeed.get(truckDriver);
			if(averageSpeed != null) {
				infractionDetail.setAverageSpeed(averageSpeed);
				LOG.info("Joined Infraction Detail["+ infractionDetail + "] with the speed ["+averageSpeed+"]");
			} else {
				LOG.info("No Speed found for Infraction Detail["+ infractionDetail);
			}
			collector.emit(new Values(truckDriver.getDriverId(), infractionDetail));
		}
		
	}


	@Override
	 public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        
		declarer.declare(new Fields("driverId", "truckDriverInfractionDetail"));
	    
	 }	



}
