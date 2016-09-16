package hortonworks.hdp.refapp.trucking.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TruckDriverInfractionDetail implements Serializable {
	
	
	private static final long serialVersionUID = 314833351299038521L;
	private static final int HIGH_SPEED = 80;

	private TruckDriver truckDriver;
	private List<InfractionCount> infractions = new ArrayList<InfractionCount>();
	private int averageSpeed = 0;
	
	public List<InfractionCount> getInfractions() {
		return infractions;
	}

	public void setInfractions(List<InfractionCount> infractions) {
		this.infractions = infractions;
	}

	public TruckDriverInfractionDetail(TruckDriver truckDriver) {
		super();
		this.truckDriver = truckDriver;
	}

	public void addInfraction(String eventType) {
		InfractionCount infractionDetail = getInfractionCount(eventType);
		if(infractionDetail == null) {
			infractionDetail = new InfractionCount(eventType);
			infractions.add(infractionDetail);
		}
		infractionDetail.increment();
	}
	
	
	public TruckDriver getTruckDriver() {
		return truckDriver;
	}

	public void setTruckDriver(TruckDriver truckDriver) {
		this.truckDriver = truckDriver;
	}


	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(truckDriver.toString());
		for(InfractionCount infractionDetail: infractions) {
			buffer.append(", " + infractionDetail.toString());
		}
		return buffer.toString();
	}
	
	public InfractionCount getInfractionCount(String eventType) {
		for(InfractionCount count: infractions) {
			if(count.getInfractionEvent().equals(eventType)) {
				return count;
			}
		}
		return null;
	}

	public int getAverageSpeed() {
		return averageSpeed;
	}

	public void setAverageSpeed(int averageSpeed) {
		this.averageSpeed = averageSpeed;
	}
	
	public boolean isDriverSpeeding() {
		return averageSpeed > HIGH_SPEED;
	}
	
}
