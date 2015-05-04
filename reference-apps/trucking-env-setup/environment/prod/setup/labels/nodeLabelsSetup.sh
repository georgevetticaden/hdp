#/bin/bash

# Execute this as root


echo "Creating Labels Directory"
runuser -l  hdfs -c "hadoop fs -mkdir -p /yarn/node-labels"
runuser -l  hdfs -c "hadoop fs -chown -R yarn:yarn /yarn"
runuser -l  hdfs -c "hadoop fs -chmod -R 700 /yarn"


echo "Creating yarn home directory"
runuser -l  hdfs -c 'hadoop fs -mkdir /user/yarn'
runuser -l  hdfs -c 'hadoop fs -chown yarn:hdfs /user/yarn'
echo "Done creating yarn home directory"
