package hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport;

import hortonworks.hdp.refapp.trucking.simulator.impl.domain.Event;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.gps.Location;


public class MobileEyeEvent extends Event {
	private MobileEyeEventTypeEnum eventType;
	private Truck truck;
	private Location location;
	private long correlationId;
	
	private int truckSpeed;



	public int getTruckSpeed() {
		return truckSpeed;
	}

	public void setTruckSpeed(int truckSpeed) {
		this.truckSpeed = truckSpeed;
	}

	public MobileEyeEvent(long correlationId, Location location, MobileEyeEventTypeEnum eventType,
			Truck truck, int truckSpeed) {
		this.location = location;
		this.eventType = eventType;
		this.truck = truck;
		this.correlationId = correlationId;
		this.truckSpeed= truckSpeed;
	}

	public MobileEyeEventTypeEnum getEventType() {
		return eventType;
	}

	public void setEventType(MobileEyeEventTypeEnum eventType) {
		this.eventType = eventType;
	}

	public Location getLocation() {
		return location;
	}
	
	public Truck getTruck() {
		return this.truck;
	}

	@Override
	public String toString() {
		return truck.toString() + eventType.toString() + "|"
				+ location.getLatitude() + "|" + location.getLongitude() + "|" + correlationId + "|";
	}
}
