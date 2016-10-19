package hortonworks.hdp.refapp.trucking.simulator.impl.collectors;

import java.io.Serializable;

public class TruckSpeedEvent implements Serializable {
	
	private String eventTime;
	private String eventSource;
	private Integer truckId;
	private Integer driverId;
	private String driverName;
	private Integer routeId;
	private String route;
	private Integer speed;
	public String getEventTime() {
		return eventTime;
	}
	public void setEventTime(String eventTime) {
		this.eventTime = eventTime;
	}
	public String getEventSource() {
		return eventSource;
	}
	public void setEventSource(String eventSource) {
		this.eventSource = eventSource;
	}
	public Integer getTruckId() {
		return truckId;
	}
	public void setTruckId(Integer truckId) {
		this.truckId = truckId;
	}
	public Integer getDriverId() {
		return driverId;
	}
	public void setDriverId(Integer driverId) {
		this.driverId = driverId;
	}
	public String getDriverName() {
		return driverName;
	}
	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}
	public Integer getRouteId() {
		return routeId;
	}
	public void setRouteId(Integer routeId) {
		this.routeId = routeId;
	}
	public String getRoute() {
		return route;
	}
	public void setRoute(String route) {
		this.route = route;
	}
	public Integer getSpeed() {
		return speed;
	}
	public void setSpeed(Integer speed) {
		this.speed = speed;
	}
	
	
	
}
