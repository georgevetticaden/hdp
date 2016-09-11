create table truck_events_text_partition_single(
      driverId         int
,     truckId         int
,     eventTime       string
,     eventType          string
,     longitude         double
,     latitude           double
,     eventKey        string
,     correlationId     bigint
,     driverName        string
,     routeId           int
,     routeName         string
)
partitioned by (dateTruck string)
row format delimited fields terminated by ',';