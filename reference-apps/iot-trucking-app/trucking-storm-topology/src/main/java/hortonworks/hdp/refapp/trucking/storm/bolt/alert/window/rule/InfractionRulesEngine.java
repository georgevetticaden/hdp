package hortonworks.hdp.refapp.trucking.storm.bolt.alert.window.rule;

import hortonworks.hdp.refapp.trucking.domain.InfractionCount;
import hortonworks.hdp.refapp.trucking.domain.TruckDriver;
import hortonworks.hdp.refapp.trucking.domain.TruckDriverInfractionDetail;
import hortonworks.hdp.refapp.trucking.domain.TruckDriverInfractionsNotification;
import hortonworks.hdp.refapp.trucking.storm.util.EventMailer;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class InfractionRulesEngine implements Serializable {
	
	private static final long serialVersionUID = -5526455911057368428L;
	private static final Logger LOG = LoggerFactory.getLogger(InfractionRulesEngine.class);

	private static final long ACTIVEMQ_MESSAGE_TTL = 10000;
	
	

	private String email ;
	private String subject;
	private EventMailer eventMailer;
	private boolean sendAlertToEmail;
	private boolean sendAlertToTopic;
	
	private String user;
	private String password;
	private String activeMQConnectionString;
	private String topicName;
	
	
	private Session session = null;
	private Connection connection = null;
	private ActiveMQConnectionFactory connectionFactory = null;
    private HashMap<String, MessageProducer> producers = new HashMap<String, MessageProducer>();	
	

	public InfractionRulesEngine(Properties config) {
		
		this.sendAlertToEmail = Boolean.valueOf(config.getProperty("trucking.notification.email")).booleanValue();
		if(sendAlertToEmail) {
			LOG.info("TruckEventRuleEngine configured to send email on alert");
			configureEmail(config);		
		} else {
			LOG.info("TruckEventRuleEngine configured to NOT send alerts");
		}
		
		this.sendAlertToTopic = Boolean.valueOf(config.getProperty("trucking.notification.topic")).booleanValue();
		
		if(sendAlertToTopic) {
			this.user = config.getProperty("trucking.notification.topic.user");
			this.password = config.getProperty("trucking.notification.topic.password");
			this.activeMQConnectionString = config.getProperty("trucking.notification.topic.connection.url");
			this.topicName = config.getProperty("trucking.notification.topic.alerts.name");	
			
			initializeJMSConnection();
			
			
		} else {
			LOG.info("TruckEventRuleEngine not configured to send alerts to Topic");
		}
	}


	public void processEvent(TruckDriverInfractionDetail infractionDetail) {
		LOG.debug("Rule being executed for: " + infractionDetail);
		executeRule_2InfractionsAcross2AlertTypes(infractionDetail);
		executeRule_sustainedHighSpeedDrivining(infractionDetail);
		
	}	
	

	private void executeRule_sustainedHighSpeedDrivining(
			TruckDriverInfractionDetail infractionDetail) {
		
		List<InfractionCount> infractionCounts = infractionDetail.getInfractions();
		
		/* If there aren't atleast 2 different infraction events, then don't alert */
		
		if(infractionDetail.isDriverSpeeding()) {
			if(sendAlertToTopic) {
				String alertMessage = "Driver["+infractionDetail.getTruckDriver() + "] has been speeeding at "+infractionDetail.getAverageSpeed()+" for the last 5 minutes";
				sendAlertToTopic("High Speed Driving for Sustained Period",alertMessage, infractionDetail);	
			}				
		} else {
			LOG.info("Rule didn't fire because Driver["+infractionDetail.getTruckDriver() + "] is not speeding["+ infractionDetail.getAverageSpeed() +"]");

		}
		
	}



	/**
	 * If Driver has atleast 2 of the same infractions across 2 different event types, then trigger an alert
	 * @param infractionCount
	 */
	private void executeRule_2InfractionsAcross2AlertTypes(TruckDriverInfractionDetail infractionDetail) {
		
		
		List<InfractionCount> infractionCounts = infractionDetail.getInfractions();
		
		/* If there aren't atleast 2 different infraction events, then don't alert */
		if(infractionCounts.size() < 2) {
			LOG.debug("Rule[2InfractionsAcross2AlertTypes] didn't fire because the number of infrations["+infractionCounts.size()+"] was less than 2");
			return;
		}
			
		
		List<InfractionCount> infractionCountsOfInterest = new ArrayList<InfractionCount>();
			
		for(InfractionCount infractionCount : infractionCounts) {
			if(infractionCount.getCount() > 1) {
				infractionCountsOfInterest.add(infractionCount);
			}
		}
		
		/* If there were atleast 2 infractions per infraction event across atleast 2 different events then alert */
		if(infractionCountsOfInterest.size() > 1) {
			
			if(sendAlertToTopic) {
				TruckDriverInfractionDetail alertObject = new TruckDriverInfractionDetail(infractionDetail.getTruckDriver());
				alertObject.setInfractions(infractionCountsOfInterest);

				String alertMessage = "Driver["+infractionDetail.getTruckDriver() + "] has alleast 2 infractions across atleast 2 different infraction types in the last 3 minutes";
				

				sendAlertToTopic("Multiple Infractions",alertMessage, alertObject);	
			}	
		} else {
			LOG.debug("Rule[2InfractionsAcross2AlertTypes] didn't fire becuase the number of infractions for atleast 2 infrations was not greater than 2: " + infractionCounts );
		}

	}


	
	private void configureEmail(Properties config) {
		this.eventMailer = new EventMailer(config);			
		this.email = config.getProperty("trucking.notification.email.address");
		this.subject =  config.getProperty("trucking.notification.email.subject");
		LOG.info("Initializing rule engine with email: " + email
				+ " subject: " + subject);
	}

	
	private void sendAlertToTopic(String alertName, String alertMessage, TruckDriverInfractionDetail infractionDetail) {
		TruckDriver truckDriver = infractionDetail.getTruckDriver();
		String truckDriverEventKey = truckDriver.getDriverId() + "|" + truckDriver.getTruckId();
		SimpleDateFormat sdf = new SimpleDateFormat();
		String timeStampString = sdf.format(new Date().getTime());		
		
		TruckDriverInfractionsNotification notification = new TruckDriverInfractionsNotification(alertName, timeStampString, alertMessage, infractionDetail);
		
		String jsonAlert;
		try {
			ObjectMapper mapper = new ObjectMapper();
			jsonAlert = mapper.writeValueAsString(notification);
			
			LOG.info("Rule["+ alertName +"] fired. About to send the following to Alerts Notification Queue: " + jsonAlert);
			
		} catch (Exception e) {
			LOG.error("Error converting TruckDriverInfractionsNotification to JSON", e);
			return;
		}
		
		sendAlert(jsonAlert);		
		
	}
	

	
	
	private void sendAlert(String event) {
		try {
            TextMessage message = session.createTextMessage(event);
			//getTopicProducer(sessio   n, topic).send(message);
			MessageProducer producer = producers.get(this.topicName);
			producer.send(message, producer.getDeliveryMode(), producer.getPriority(), ACTIVEMQ_MESSAGE_TTL);
		} catch (JMSException e) {
			LOG.error("Error sending TruckDriverViolationEvent to topic", e);
			return;
		}
	}	

	private void sendAlertEmail(String driverName, int driverId, StringBuffer events) {
		eventMailer.sendEmail(email, email, subject,
				"We've identified 5 unsafe driving events for Driver " + driverName + " with Driver Identification Number: "
						+ driverId + "\n\n" + events.toString());
	}
	
	
	public void cleanUpResources() {

	}

	private void initializeJMSConnection() {
		try{
			connectionFactory = new ActiveMQConnectionFactory(user, password,activeMQConnectionString);
			connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            producers.put(this.topicName, getTopicProducer(session, this.topicName));
            
            LOG.info("JMS connection to topic["+topicName +"] successful");
		}
		catch (JMSException e) {
			LOG.error("Error sending to topic["+this.topicName + "]", e);
			return;
		}
	}
	
	private MessageProducer getTopicProducer(Session session, String topic) {
		try {
			Topic topicDestination = session.createTopic(topic);
			MessageProducer topicProducer = session.createProducer(topicDestination);
			topicProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			return topicProducer;
		} catch (JMSException e) {
			LOG.error("Error creating producer for topic", e);
			throw new RuntimeException("Error creating producer for topic");
		}
	}		


}
