package hortonworks.hdp.refapp.trucking.domain;

import java.io.Serializable;


public class TruckDriver implements Serializable {
	

	private static final long serialVersionUID = 1652090883008513888L;
	
	private int driverId;
	private int truckId;
	private String driverName;
	private String routeName;
	
	public int getDriverId() {
		return driverId;
	}


	public void setDriverId(int driverId) {
		this.driverId = driverId;
	}


	public int getTruckId() {
		return truckId;
	}


	public void setTruckId(int truckId) {
		this.truckId = truckId;
	}


	
	public TruckDriver(int driverId, String driverName, int truckId, String route) {
		super();
		this.driverId = driverId;
		this.truckId = truckId;
		this.driverName = driverName;
		this.routeName = route;
	}

	
	@Override
	public boolean equals(Object containerObject) {
		TruckDriver container = (TruckDriver) containerObject;
		return this.driverId == container.driverId && this.truckId == container.truckId;
	}
	
	@Override
	public int hashCode() {
		int hash = 3;
		hash = 53 * hash + driverId;
		hash = 53 * hash + truckId;
		return hash;
	}
	
	
	@Override
	public String toString() {
		return "driver["+driverId + "], truck[" + truckId +"]";
	}


	public String getRouteName() {
		return routeName;
	}


	public void setRouteName(String routeName) {
		this.routeName = routeName;
	}


	public String getDriverName() {
		return driverName;
	}


	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}
	
}	