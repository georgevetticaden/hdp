package hortonworks.hdp.refapp.trucking.monitor.web;

import hortonworks.hdp.refapp.trucking.monitor.DriverEventsResponse;
import hortonworks.hdp.refapp.trucking.monitor.StreamGeneratorParam;
import hortonworks.hdp.refapp.trucking.monitor.service.TruckDemoService;

import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


@Controller
public class TruckMonitorController {

	private static final Logger LOG = LoggerFactory.getLogger(TruckMonitorController.class);
	
	public static final double STL_LAT= 38.523884;
	public static final double STL_LONG= -92.159845;
	public static final int DEFAULT_ZOOME_LEVEL = 6;
	public static final int DEFAULT_TRUCK_SYMBOL_SIZE = 10000;	

	@Autowired
	private TruckDemoService truckDemoService;
	
	@Autowired
	private ApplicationContext appContext;	


	
	/**
	 * Returns all the driver events from HBase asynchronously when the monitor view is rendered.
	 * This will be called asyncrhohnsuly via a jquery ajax call
	 * @param session 
	 * @return
	 * @throws Exception
	 */
	@RequestMapping("/iotdemo/truck/monitor/driverEvents.json")
	public DriverEventsResponse getDriverEvents(HttpSession session) throws Exception {
		LOG.info("Fetching Driver Events Asynchronously");
		
		DriverEventsResponse response = new DriverEventsResponse();
		response.setViolationEvents(truckDemoService.getLatestEventsForAllDrivers());
		response.setStartLat(STL_LAT);
		response.setStartLong(STL_LONG);
		response.setZoomLevel(DEFAULT_ZOOME_LEVEL);
		response.setTruckSymbolSize(DEFAULT_TRUCK_SYMBOL_SIZE);
		
		LOG.info("Completed Fetching Driver Events Asynchronously");
		
		return response;
	}	
	
	/**
	 * Generates a streaming of truckign events
	 * @param param
	 * @param session 
	 * @return
	 */
	@RequestMapping(value="/iotdemo/truck/monitor/streaming",  method=RequestMethod.POST)
	public String generateTruckStreams() {
		LOG.info("Inside generate Truck Streams");
		String result = "SUCCESS";
		try {
			
			StreamGeneratorParam streamConfig = constructDefaultStreamGeneratorParam();
			truckDemoService.generateTruckEventsStream(streamConfig);
		} catch (Exception e) {
			LOG.error("Error Generating Truck Streams", e);
			result = "ERROR";
		}
		return result;
	}		
	


	/*
	 * Default Values for the the STream Generator Page
	 */
	private StreamGeneratorParam constructDefaultStreamGeneratorParam() {
		StreamGeneratorParam param =  new StreamGeneratorParam();
		param.setEventEmitterClassName("hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.Truck");
		param.setEventCollectorClassName("hortonworks.hdp.refapp.trucking.simulator.impl.collectors.KafkaEventCollector");
		String routePath = "";
		try {
			routePath = appContext.getResource("classpath:/routes/midwest").getURI().getPath();
		} catch (Exception e) {
			LOG.error("Error loading routes directory", e);
		}
		param.setRouteDirectory(routePath);
		param.setCenterCoordinatesLat(38.523884);
		param.setCenterCoordinatesLong(-92.159845);
		param.setDelayBetweenEvents(1000);
		param.setNumberOfEvents(200);
		param.setTruckSymbolSize(10000);
		param.setZoomLevel(7);
		return param;
	}
	
	

}
