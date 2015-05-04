package hortonworks.hdp.refapp.trucking.simulator.impl.domain.transport.route;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import hortonworks.hdp.refapp.trucking.simulator.impl.domain.gps.Location;

import java.util.List;

import org.junit.Test;

public class DriverRouteParserTest {
	
	@Test
	public void parseRoute() {
		TruckRoutesParser parser = new TruckRoutesParser();
		String file = "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/stream-simulator/src/test/resources/TestRoute2.xml";
		Route route =  parser.parseRoute(file);
		assertNotNull(route);
		List<hortonworks.hdp.refapp.trucking.simulator.impl.domain.gps.Location> locations = route.getLocations();
		assertNotNull(locations);
		assertEquals(16,  locations.size());
		for(Location location : locations) {
			System.out.println("Lat:"  + location.getLatitude());
			System.out.println("Long:" + location.getLongitude());
		}
		
	}
	
	@Test
	public void parseRoutes() {
		TruckRoutesParser parser = new TruckRoutesParser();
		List<Route> routes = parser.parseAllRoutes("/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/stream-simulator/src/test/resources");
		assertNotNull(routes);
		assertEquals(2, routes.size());
		
		for (Route route:routes) {
			List<Location> locations = route.getLocations();
			assertNotNull(locations);
			for(Location location : locations) {
				System.out.println("Lat:"  + location.getLatitude());
				System.out.println("Long:" + location.getLongitude());
			}
		}
	}

}
