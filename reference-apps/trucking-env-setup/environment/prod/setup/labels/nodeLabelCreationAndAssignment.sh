#/bin/bash

# Execute this as root

echo "Creating Labels"
runuser -l  yarn -c "yarn rmadmin -addToClusterNodeLabels 'hbase,storm'"


echo "Assigning Labels to Nodes"
runuser -l yarn -c "yarn rmadmin -replaceLabelsOnNode 'vetticaden03.cloud.hortonworks.com,hbase vetticaden04.cloud.hortonworks.com,hbase vetticaden05.cloud.hortonworks.com,storm vetticaden06.cloud.hortonworks.com,storm'"


echo "Listing All Node Labels"
runuser -l yarn -c "yarn cluster --list-node-labels"