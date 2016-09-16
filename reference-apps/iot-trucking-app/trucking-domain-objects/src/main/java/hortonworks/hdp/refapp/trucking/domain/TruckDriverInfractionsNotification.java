package hortonworks.hdp.refapp.trucking.domain;

import java.io.Serializable;
import java.util.List;

public class TruckDriverInfractionsNotification implements Serializable {


	private static final long serialVersionUID = 8678511542622937121L;
	
	
	private String notificationTimestamp;
	private String notificationMessage;
	private TruckDriverInfractionDetail infractionDetail;
	


	public TruckDriverInfractionsNotification(String alertName, String notificationTimestamp,
			String notificationMessage, TruckDriverInfractionDetail infractionDetail) {
		super();
		this.alertName = alertName;
		this.notificationTimestamp = notificationTimestamp;
		this.notificationMessage = notificationMessage;
		this.infractionDetail = infractionDetail;
	}
	
		

	private String alertName;
	
	public String getAlertName() {
		return alertName;
	}

	public void setAlertName(String alertName) {
		this.alertName = alertName;
	}

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
	
	public TruckDriverInfractionDetail getInfractionDetail() {
		return infractionDetail;
	}

	public void setInfractionDetail(TruckDriverInfractionDetail infractionDetail) {
		this.infractionDetail = infractionDetail;
	}	
	

}
