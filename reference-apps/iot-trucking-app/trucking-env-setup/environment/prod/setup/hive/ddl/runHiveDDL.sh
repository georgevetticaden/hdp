echo "Creating Hive Tables"
runuser -l  hdfs -c 'hive -f truck_events_text_partition_single.ddl'
runuser -l  hdfs -c 'hive -f truck_events_orc_partition_single.ddl'
echo "Done Creating Hive Tables"

echo "Changing Permissions on Created Hive Tables"

runuser -l  hdfs -c 'hadoop fs -chown yarn:hadoop /apps/hive/warehouse/truck_events_text_partition_single'
runuser -l  hdfs -c ' hadoop fs -chown hdfs:hadoop /apps/hive/warehouse/truck_events_orc_partition_single'

runuser -l  hdfs -c 'hadoop fs -chmod 770 /apps/hive/warehouse/truck_events_text_partition_single'
runuser -l  hdfs -c 'hadoop fs -chmod 770 /apps/hive/warehouse/truck_events_orc_partition_single'

echo "Finished Changing Permissions on Created Hive Tables"

