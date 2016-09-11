package hortonworks.hdp.refapp.trucking.storm.bolt.alert;

import hortonworks.hdp.refapp.trucking.domain.DriverAlertNotification;
import hortonworks.hdp.refapp.trucking.storm.util.EventMailer;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
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



public class TruckEventRuleEngine implements Serializable {
	
	private static final long serialVersionUID = -5526455911057368428L;
	private static final Logger LOG = LoggerFactory.getLogger(TruckEventRuleEngine.class);
	public static final int MAX_UNSAFE_EVENTS = 5;
	
	public Map<Integer, LinkedList<String>> driverEvents = new HashMap<Integer, LinkedList<String>>();
	private long lastCorrelationId;
	
	private String email ;
	private String subject;
	private EventMailer eventMailer;
	private boolean sendAlertToEmail;
	private boolean sendAlertToTopic;
	
	private String user;
	private String password;
	private String activeMQConnectionString;
	private String topicName;
	

	public TruckEventRuleEngine(Properties config) {
		
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
			
			
		} else {
			LOG.info("TruckEventRuleEngine configured to alerts to Topic");
		}
	}


	public void processEvent(int driverId, String driverName, int routeId, int truckId, Timestamp eventTime, String event, double longitude, double latitude, long currentCorrelationId, String routeName) {

		if(lastCorrelationId != currentCorrelationId) {
			lastCorrelationId = currentCorrelationId;
			driverEvents.clear();
		}
		if (!driverEvents.containsKey(driverId))
			driverEvents.put(driverId, new LinkedList<String>());
			
		
		if (!event.equals("Normal")) {
			if (driverEvents.get(driverId).size() < MAX_UNSAFE_EVENTS) {
				driverEvents.get(driverId).push(eventTime + " " + event);
				LOG.info("Driver["+driverId+"] " + driverName + " has an unsafe event and now has the following unsfae events " + driverEvents.get(driverId).size());
			}
			else {
				LOG.info("Driver["+driverId+"] has exceed max events...");
				try {
					// In this case they've had more than 5 unsafe events
					LOG.info("UNSAFE DRIVING DETECTED FOR DRIVER ID: "
							+ driverId);
					StringBuffer events = new StringBuffer();
					for (String unsafeEvent : driverEvents.get(driverId)) {
						events.append(unsafeEvent + "\n");
					}
					
					if(sendAlertToEmail)
						sendAlertEmail(driverName, driverId, events);
					
					if(sendAlertToTopic)
						sendAlertToTopic(driverName, driverId, events, truckId, eventTime.getTime(), routeId, routeName);
				} catch (Exception e) {
					LOG.error("Error occured while sending notificaiton email: "
							+ e.getMessage());
				} finally {
					driverEvents.get(driverId).clear();
				}
			}
		}
	}
	
	private void configureEmail(Properties config) {
		this.eventMailer = new EventMailer(config);			
		this.email = config.getProperty("trucking.notification.email.address");
		this.subject =  config.getProperty("trucking.notification.email.subject");
		LOG.info("Initializing rule engine with email: " + email
				+ " subject: " + subject);
	}

	private void sendAlertToTopic(String driverName, int driverId, StringBuffer events, int truckId, long timeStamp, int routeId, String routeName) {
		String truckDriverEventKey = driverId + "|" + truckId;
		SimpleDateFormat sdf = new SimpleDateFormat();
		String timeStampString = sdf.format(timeStamp);
		
		String alertMessage = "5 unsafe driving events have been identified for Driver " + driverName + " with Driver Identification Number: " + driverId + " for Route["+routeName + "] " 
								+ events.toString();
		
		DriverAlertNotification alert = new DriverAlertNotification(truckDriverEventKey, driverId, truckId, 
											timeStamp, timeStampString, alertMessage, driverName, routeId, routeName);

		String jsonAlert;
		try {
			ObjectMapper mapper = new ObjectMapper();
			jsonAlert = mapper.writeValueAsString(alert);
		} catch (Exception e) {
			LOG.error("Error converting DriverAlertNotification to JSON", e);
			return;
		}
		
		sendAlert(jsonAlert);
		
	}
	
	private void sendAlert(String event) {
		Session session = null;
		try {
			session = createSession();
			TextMessage message = session.createTextMessage(event);
			getTopicProducer(session).send(message);
		} catch (JMSException e) {
			LOG.error("Error sending TruckDriverViolationEvent to topic", e);
			return;
		}finally{
			if(session != null) {
				try {
					session.close();
				} catch (JMSException e) {
					LOG.error("Error cleaning up ActiveMQ resources", e);
				}				
			}

		}
	}	

	private void sendAlertEmail(String driverName, int driverId, StringBuffer events) {
		eventMailer.sendEmail(email, email, subject,
				"We've identified 5 unsafe driving events for Driver " + driverName + " with Driver Identification Number: "
						+ driverId + "\n\n" + events.toString());
	}
	
	
	private MessageProducer getTopicProducer(Session session) {
		try {
			Topic topicDestination = session.createTopic(topicName);
			MessageProducer topicProducer = session.createProducer(topicDestination);
			topicProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			return topicProducer;
		} catch (JMSException e) {
			LOG.error("Error creating producer for topic", e);
			throw new RuntimeException("Error creating producer for topic");
		}
	}	
	
	private Session createSession() {
		
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password,activeMQConnectionString);
			Connection connection = connectionFactory.createConnection();
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			return session;
		} catch (JMSException e) {
			LOG.error("Error configuring ActiveMQConnection and getting session", e);
			throw new RuntimeException("Error configuring ActiveMQConnection");
		}
	}	
	
	public void cleanUpResources() {

	}
}
