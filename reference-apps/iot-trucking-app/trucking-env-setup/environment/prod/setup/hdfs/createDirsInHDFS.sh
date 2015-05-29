echo "Creating HDFS landing directory for truck events"
runuser -l  hdfs -c 'hadoop fs -mkdir /truck-events-v4'
runuser -l  hdfs -c 'hadoop fs -chown yarn:hadoop /truck-events-v4'
runuser -l  hdfs -c 'hadoop fs -chmod -R  770 /truck-events-v4'
echo "Done Creating HDFS landing directory for truck events"
