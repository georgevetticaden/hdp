#/bin/bash

# Execute this as root

# Install HBase
echo "Installing Hbase"
wget  http://public-repo-1.hortonworks.com/HDP/centos6/2.x/GA/2.2.0.0/slider-app-packages/hbase/slider-hbase-app-package-0.98.4.2.2.0.0-2041-hadoop2.zip
mv slider-hbase-app-package-0.98.4.2.2.0.0-2041-hadoop2.zip /var/lib/ambari-server/resources/apps
echo "Done Installing Hbase"

# Install Storm
echo "Installing Storm"
wget  http://public-repo-1.hortonworks.com/HDP/centos6/2.x/GA/2.2.0.0/slider-app-packages/storm/slider-storm-app-package-0.9.3.2.2.0.0-2041.zip
mv slider-storm-app-package-0.9.3.2.2.0.0-2041.zip /var/lib/ambari-server/resources/apps
echo "Done Installing Hbase"


#restart ambari
echo "Restarting Ambari"
ambari-server restart
echo "Done Restarting Ambari"