package hortonworks.hdp.refapp.trucking.storm.bolt.alert;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TruckDriverInfractionCount implements Serializable {
	
	
	private static final long serialVersionUID = 314833351299038521L;

	private TruckDriver truckDriver;
	private Map<String, Integer> infractionCountByEventType = new HashMap<String, Integer>();
	
	
	public TruckDriverInfractionCount(TruckDriver truckDriver) {
		super();
		this.truckDriver = truckDriver;
	}

	public void addInfraction(String eventType) {
		Integer count = infractionCountByEventType.get(eventType);
		if(count == null) count = 0;
		infractionCountByEventType.put(eventType, count + 1);
	}
	
	
	public TruckDriver getTruckDriver() {
		return truckDriver;
	}

	public void setTruckDriver(TruckDriver truckDriver) {
		this.truckDriver = truckDriver;
	}

	public Map<String, Integer> getInfractionCountByEventType() {
		return infractionCountByEventType;
	}

	public void setInfractionCountByEventType(
			Map<String, Integer> infractionCountByEventType) {
		this.infractionCountByEventType = infractionCountByEventType;
	}

	
	
	

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(truckDriver.toString());
		for(String eventType: infractionCountByEventType.keySet()) {
			buffer.append(", [ Event Type: "+eventType +" , Count: "+infractionCountByEventType.get(eventType) + "]");
		}
		return buffer.toString();
	}
	
	
}
