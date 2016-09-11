package hortonworks.hdp.refapp.trucking.config.web;



import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.trucking.config.app.AppConfig;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.config.StompBrokerRelayRegistration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;


@Configuration
@org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker
@EnableScheduling
@ComponentScan(basePackages="hortonworks.hdp.refapp")
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {

	private static final Logger LOG = LoggerFactory.getLogger(WebSocketConfig.class);
	@Autowired
	private AppConfig appConfig;
	
	@Autowired
	HDPServiceRegistry serviceRegistry;

	@Override
	public void registerStompEndpoints(StompEndpointRegistry registry) {
		registry.addEndpoint("/monitor").withSockJS();
	}

	
	
	@Override
	public void configureMessageBroker(MessageBrokerRegistry registry) {
		StompBrokerRelayRegistration registration = registry.enableStompBrokerRelay("/queue", "/topic");
		String activeMQHost = System.getProperty("trucking.activemq.host");
		if(StringUtils.isEmpty(activeMQHost)) {
			String errMsg = "Property[trucking.activemq.host] in ref-app-hdp-service-config.properties must be configured for WebSocket";
			LOG.error(errMsg);
			throw new  RuntimeException(errMsg);
		}
		registration.setRelayHost(activeMQHost);
		registration.setRelayPort(61613);
		registry.setApplicationDestinationPrefixes("/app");
	}


	@Override
	public void configureClientInboundChannel(ChannelRegistration inboundChannel) {
		
	}

	@Override
	public void configureClientOutboundChannel(ChannelRegistration out) {
		
	}
	

}
