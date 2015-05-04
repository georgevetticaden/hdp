package hortonworks.hdp.refapp.trucking.storm.bolt.solr;


import java.util.Date;

import org.apache.solr.client.solrj.beans.Field;

public class TruckEvent {
	
	@Field
	String id;
	
	@Field
	int driverid;
	
	@Field
	int truckid;
	
	@Field
	Date time;				
	
	@Field
	String eventtype;
	
	@Field
	double longitude;
	
	@Field
	double latitude;
	
	@Field
	String drivername;
	
	@Field
	int routeid;
	
	@Field
	String routename ;

	public TruckEvent(String id, int driverid, int truckid,
			Date time, String eventtype, double longitude, double latitude,
			String drivername, int routeid, String routename) {
		super();
		this.id = id;
		this.driverid = driverid;
		this.truckid = truckid;
		this.time = time;
		this.eventtype = eventtype;
		this.longitude = longitude;
		this.latitude = latitude;
		this.drivername = drivername;
		this.routeid = routeid;
		this.routename = routename;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}


	public int getDriverid() {
		return driverid;
	}

	public void setDriverid(int driverid) {
		this.driverid = driverid;
	}

	public int getTruckid() {
		return truckid;
	}

	public void setTruckid(int truckid) {
		this.truckid = truckid;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public String getEventtype() {
		return eventtype;
	}

	public void setEventtype(String eventtype) {
		this.eventtype = eventtype;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public String getDrivername() {
		return drivername;
	}

	public void setDrivername(String drivername) {
		this.drivername = drivername;
	}

	public int getRouteid() {
		return routeid;
	}

	public void setRouteid(int routeid) {
		this.routeid = routeid;
	}

	public String getRoutename() {
		return routename;
	}

	public void setRoutename(String routename) {
		this.routename = routename;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("id:").append(id).append(", ");
		buffer.append("driverid:").append(driverid).append(", ");
		buffer.append("truckid:").append(truckid).append(", ");
		buffer.append("time:").append(time).append(", ");
		buffer.append("eventtype:").append(eventtype).append(", ");

		buffer.append("longitude:").append(longitude).append(", ");
		buffer.append("latitude:").append(latitude).append(", ");		
		
		buffer.append("drivername").append(drivername).append(", ");
		buffer.append("routeid").append(routeid).append(", ");
		buffer.append("routename").append(routename).append(", ");
		
		return buffer.toString();
	}

}
