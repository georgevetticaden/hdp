package hortonworks.hdp.refapp.trucking.domain;

import java.io.Serializable;

public class InfractionCount implements Serializable {

	private static final long serialVersionUID = -8895492606967188122L;

	private int count = 0;
	private String infractionEvent;
	
	public InfractionCount(String infractionEvent) {
		super();
		this.infractionEvent = infractionEvent;
	}
	
	public void increment() {
		count++;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getInfractionEvent() {
		return infractionEvent;
	}

	public void setInfractionEvent(String infractionEvent) {
		this.infractionEvent = infractionEvent;
	}
	
	@Override
	public String toString() {
		return "[ Event Type: "+infractionEvent +" , Count: "+count + "]";
	}
	
}
