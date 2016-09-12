package hortonworks.hdp.refapp.trucking.domain;

import java.io.Serializable;
import java.util.List;

public class TruckDriverInfractionsNotification implements Serializable {


	private static final long serialVersionUID = 8678511542622937121L;
	
	
	private String notificationTimestamp;
	private String notificationMessage;
	private List<InfractionCount> infractions;
	
	public String getNotificationTimestamp() {
		return notificationTimestamp;
	}

	public void setNotificationTimestamp(String notificationTimestamp) {
		this.notificationTimestamp = notificationTimestamp;
	}

	public String getNotificationMessage() {
		return notificationMessage;
	}

	public void setNotificationMessage(String notificationMessage) {
		this.notificationMessage = notificationMessage;
	}

	public List<InfractionCount> getInfractions() {
		return infractions;
	}

	public void setInfractions(List<InfractionCount> infractions) {
		this.infractions = infractions;
	}

	public TruckDriverInfractionsNotification(String notificationTimestamp,
			String notificationMessage, List<InfractionCount> infractions) {
		super();
		this.notificationTimestamp = notificationTimestamp;
		this.notificationMessage = notificationMessage;
		this.infractions = infractions;
	}
	
	
	
	

}
