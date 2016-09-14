package hortonworks.hdp.refapp.trucking.storm.bolt.alert.window;

import java.util.ArrayList;
import java.util.List;

import hortonworks.hdp.refapp.trucking.domain.TruckDriver;

public class TruckDriverSpeeds
{

	private TruckDriver truckDriver;
	private List<Integer> speeds = new ArrayList<Integer>();

	public TruckDriverSpeeds(TruckDriver driver) {
		this.truckDriver = driver;
	}
	
	public void addSpeed(int speed) {
		speeds.add(speed);
	}
	
	public int calculateAverageSpeed() {
		if(speeds.isEmpty()) return 0;
		int speedSum = 0;
		for(Integer speed:speeds) {
			speedSum  = speed + speedSum;
		}
		return speedSum/speeds.size();
	}

	public TruckDriver getTruckDriver() {
		return truckDriver;
	}

	public void setTruckDriver(TruckDriver truckDriver) {
		this.truckDriver = truckDriver;
	}

	public List<Integer> getSpeeds() {
		return speeds;
	}

	public void setSpeeds(List<Integer> speeds) {
		this.speeds = speeds;
	}

	
	
}
