package hortonworks.hdp.refapp.trucking.storm.util;


import hortonworks.hdp.refapp.trucking.domain.TruckDriver;
import hortonworks.hdp.refapp.trucking.domain.TruckDriverInfractionDetail;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class UtilTest {
	
	@Test
	public void createTimeStamp() {
		String value = "2016-09-08 07:07:11.358";
		Timestamp eventTime = Timestamp.valueOf(value);
		System.out.println(eventTime.getTime());
	}
	
	
	
	
	@Test
	public void dateToString() throws Exception
	{
		Timestamp timeStamp = new Timestamp(new Date().getTime());
		String value = timeStamp + "|";

		
		StringSerializer serializer = new StringSerializer();
		byte[] seriliazedValue = serializer.serialize("test", value);
		
		StringDeserializer deserializer = new StringDeserializer();
		String deserializedValue= deserializer.deserialize("test", seriliazedValue);
		
		String stringDeserilaiaedValue = new String(seriliazedValue, "UTF-8");
		System.out.println(deserializedValue);
		System.out.println(stringDeserilaiaedValue);
	}
	
	@Test
	public void testCrazString() {
		String value = "truck_events���}��MW^�)14eDIVIDER2016-09-08 12:55:30.217|85|14|Adis Cesir|1384345811|Joplin to Kansas City|Normal|39.1|-94.59|1";
		String values[] = value.split("DIVIDER");
		
		System.out.println(values.length);
		System.out.println(values[0]);
		System.out.println(values[1]);
		
		String secondSplit[] = values[1].split("\\|");
		
		System.out.println(Timestamp.valueOf(secondSplit[0]));
		
	}
	
	@Test
	public void testDriverInfraction() {
		TruckDriver truckDriver1 = createTruckDriver(1, "George", 100, "STL-Chicago");
		TruckDriverInfractionDetail infractionCount1 = new TruckDriverInfractionDetail(truckDriver1);
		infractionCount1.addInfraction("Overspeed");
		infractionCount1.addInfraction("Unsafe following distance");
		infractionCount1.addInfraction("Unsafe following distance");
		System.out.println(infractionCount1.hashCode());
		
		TruckDriver truckDriver2 = createTruckDriver(2, "Darko",  200, "Chicgo-STL");
		TruckDriverInfractionDetail infractionCount2 = new TruckDriverInfractionDetail(truckDriver2);
		infractionCount2.addInfraction("Overspeed");
		infractionCount2.addInfraction("Overspeed");
		infractionCount2.addInfraction("Unsafe following distance");
		
		Map<TruckDriver, TruckDriverInfractionDetail> map = new HashMap<TruckDriver, TruckDriverInfractionDetail>();
		map.put(truckDriver1, infractionCount1);
		map.put(truckDriver2, infractionCount2);
		
		TruckDriver truckDriverOne = createTruckDriver(1, "George", 100, "STL-Chicago");
		System.out.println(map.get(truckDriverOne));
		
		TruckDriver truckDriverTwo = createTruckDriver(2, "Darko", 200,"STL-Chicago" );
		System.out.println(map.get(truckDriverTwo));		
		
		//System.out.println(infractionCount1);
//		for(String key = infractionCount1.getInfractionCountByEventType().keySet()) {
//			System.out.println(key + ":"  + infractionCount1.getInfractionCountByEventType().get(key));
//		}
		
		//System
		
	}


	private TruckDriver createTruckDriver(int driverId, String driverName, int truckId, String routeName ) {
		return new TruckDriver(driverId, driverName, truckId, routeName);
	}

	@Test
	public void date() {
		//long value = 1404096345780l;
		long value = new Date().getTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		Date date = new Date(value);
		System.out.println("date is " + dateFormat.format(date));
		
//		Calendar calendar = Calendar.getInstance();
//		calendar.setTime(date);
//		int hour =  calendar.get(Calendar.HOUR_OF_DAY);
//		System.out.println("hour is " + hour);
//		
//		System.out.println("************************ in UTC **********************");
//		SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");
//		dateFormat2.setTimeZone(TimeZone.getTimeZone("UTC"));
//		System.out.println("date in UTC is " + dateFormat2.format(date));
//		
//		Calendar calendar2 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
//		calendar2.setTime(date);
//		hour =  calendar2.get(Calendar.HOUR_OF_DAY);
//		System.out.print("hour in UTC is " + hour);
		
	}
	
	@Test
	public void timeStampDifs() throws Exception{
		Timestamp time1 = new Timestamp(new Date().getTime());
		System.out.println(time1);
		Thread.sleep(5000);
		Timestamp time2 = new Timestamp(new Date().getTime());
		
		System.out.println(time2);
		System.out.println(Math.abs(time1.getTime() - time2.getTime()));
		
		Thread.sleep(10000);
		Timestamp time3 = new Timestamp(new Date().getTime());
		System.out.println(Math.abs(time2.getTime() - time3.getTime()));
	}
	
	@Test
	public void getTime() {
		String value = "truckEventshdfs_bolt-9-6-1403818020216.txt";
		int startIndex = value.lastIndexOf("-");
		int endIndex = value.lastIndexOf(".");
		System.out.println(value.substring(startIndex + 1, endIndex));
	}
	
	@Test
	public void pathTest() {
		Path path = new Path("test");
		System.out.println(path.toString());
	}
	
	@Test
	public void time() {
		String time = "60";
		Long value = Long.valueOf(time);
		System.out.println(value);
	}
	
}
