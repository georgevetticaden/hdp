package hortonworks.hdp.refapp.trucking.monitor.web;

import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.refapp.trucking.monitor.DemoResetParam;
import hortonworks.hdp.refapp.trucking.monitor.DriverEventsResponse;
import hortonworks.hdp.refapp.trucking.monitor.StreamGeneratorParam;
import hortonworks.hdp.refapp.trucking.monitor.service.TruckDemoService;

import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;


@Controller

public class TruckDemoController {

	private static final Log LOG = LogFactory.getLog(TruckDemoController.class);
	
	public static final double STL_LAT= 38.523884;
	public static final double STL_LONG= -92.159845;
	public static final int DEFAULT_ZOOME_LEVEL = 6;
	public static final int DEFAULT_TRUCK_SYMBOL_SIZE = 10000;	

	private TruckDemoService truckDemoService;
	
	@Autowired
	private ApplicationContext appContext;	
	
	@Autowired
	private HDPServiceRegistry registry;
	
	@Autowired
	public TruckDemoController(TruckDemoService truckDemoService) {
		this.truckDemoService = truckDemoService;
	}
	

	
	/**
	 * Renders the Trucking Monitor View
	 * @return
	 */
	@RequestMapping(value="/storm/truckdemo/truckmonitor", method=RequestMethod.GET)
	public String renderTruckDriverMonitorView() {
		return "storm/truckdemo/monitor";
	}
	
	/**
	 * Returns all the driver events from HBase asynchronously when the monitor view is rendered.
	 * This will be called asyncrhohnsuly via a jquery ajax call
	 * @param session 
	 * @return
	 * @throws Exception
	 */
	@RequestMapping("/storm/truckdemo/driverEvents.json")
	public DriverEventsResponse getDriverEvents(HttpSession session) throws Exception {
		LOG.info("Fetching Driver Events Asynchronously");
		
		DriverEventsResponse response = new DriverEventsResponse();
		//response.setViolationEvents(truckDemoService.getLatestEventsForAllDriversPhoenix());
		response.setViolationEvents(truckDemoService.getLatestEventsForAllDrivers());
		response.setStartLat(STL_LAT);
		response.setStartLong(STL_LONG);
		response.setZoomLevel(DEFAULT_ZOOME_LEVEL);
		response.setTruckSymbolSize(DEFAULT_TRUCK_SYMBOL_SIZE);
		
		LOG.info("Completed Fetching Driver Events Asynchronously");
		
		return response;
	}	
	
	
	/**
	 * Render the Stream Generator View
	 * @param model
	 * @return
	 */
	@RequestMapping(value="/storm/truckdemo/streaming", method=RequestMethod.GET)
	public String renderStreamGeneratorPage(Model model) {
		LOG.info("Inside Stream Generator Render...");
		
		StreamGeneratorParam streamConfig = constructDefaultStreamGeneratorParam();
		model.addAttribute("streamConfig", streamConfig);
		
		return "storm/truckdemo/stream-generator";
	}

	/**
	 * Generates a streaming of truckign events
	 * @param param
	 * @param session 
	 * @return
	 */
	@RequestMapping(value="/storm/truckdemo/streaming",  method=RequestMethod.POST)
	public String submitStreamGeneratorPage(@ModelAttribute StreamGeneratorParam param, HttpSession session) {
		LOG.info("Starting Mapping Truck Routes...");
		truckDemoService.generateTruckEventsStream(param);
		return "redirect:/storm/truckdemo/truckmonitor";
	}	
	
	/**
	 * Renders the Demo Reset Page
	 * @param param
	 * @return
	 */
	@RequestMapping(value="/storm/truckdemo/reset", method=RequestMethod.GET)
	public String renderResetDemoPage(Model model) {
		LOG.info("Rendering Demo Reset Page");
		DemoResetParam demoParams = new DemoResetParam();
		demoParams.setTruncateHbaseTables(true);
		model.addAttribute("demoParams", demoParams);
		return "storm/truckdemo/reset-demo";
	}
	
	
	/**
	 * Resets the Storm Table truncating HBase/Phoenix Tables
	 * @param param
	 * @param session 
	 * @return
	 */	
	@RequestMapping(value="/storm/truckdemo/reset", method=RequestMethod.POST)
	public String resetDemo(@ModelAttribute DemoResetParam param, HttpSession session) {
		LOG.info("Reseting Storm Demo, truncateHBaseTables value is" + param.isTruncateHbaseTables());
		try {
			truckDemoService.resetDemo(param);
		} catch (Exception e) {
			LOG.error("Error truncating Phoenix/HBase Tables", e);
			return "storm/truckdemo/reset-demo";
		}
		
		return "redirect:/dashboard";
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
	
	private HDPServiceRegistry getServiceRegistry(HttpSession session) {
		//HDPServiceRegistry registry  = (HDPServiceRegistry) session.getAttribute("HDP.REGISTRY");
		return registry;
	}		

}
