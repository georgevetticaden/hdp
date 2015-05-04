import logging
import json
import time

from setup.common import AmbariUtils


def configurePropertiesForYarnLabels(registry, yarnSiteVersion):
    #Get Current YARN-SITE & Add Preemption Values to Dict
    orgYARNProps = (json.loads(AmbariUtils.restGET(registry, registry['clusterName']+'/configurations?type=yarn-site&tag='+yarnSiteVersion)))['items'][0]['properties']
    logging.info(orgYARNProps);
    orgYARNProps['yarn.node-labels.manager-class']='org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager'
    orgYARNProps['yarn.node-labels.fs-store.root-dir']='hdfs://vetticaden01.cloud.hortonworks.com:8020/yarn/node-labels'
    # On a Typical VM Node where you have less than 8 GB of memory, decrease the memory to 682 from 1024 or slider with node labels wont' work
    orgYARNProps['yarn.scheduler.minimum-allocation-mb']='682'
    

    #fix single quotes pythng dict issue with json.dumps
    orgYARNProps = json.dumps(orgYARNProps)
    newYARNSite = json.loads('{"Clusters": {"desired_config": {"type": "yarn-site","tag": "version'+str(int(time.time()*1000))+'", "properties": '+ orgYARNProps +'}}}')
    logging.info('newYARNSite ' + json.dumps(newYARNSite))
    AmbariUtils.restPUT(registry, registry['clusterName'], json.dumps(newYARNSite))
    

def getLatestYarnVersion(registry):
    currentConfigs = AmbariUtils.restGET(registry, registry['clusterName']+'/configurations');
    yarnSiteVersion = AmbariUtils.vectVersion(currentConfigs, 'yarn-site')
    return yarnSiteVersion


def updateYarnConfigForLabels(registry):
    
    
    # Update with new capacity scheduler config
    print ("Updating Yarn Configuration for Node Labels.")
    yarnSiteVersion = getLatestYarnVersion(registry)
    configurePropertiesForYarnLabels(registry, yarnSiteVersion);  
    
    # restart yarn to take affect
    print ("Restarting Yarn Service to Configuration to Take Affect.")
    AmbariUtils.stopYarnService(registry);
    time.sleep(int(40))
    AmbariUtils.startYarnService(registry);
    time.sleep(int(120))


# registry = AmbariUtils.createRegistry("../config.properties");
# setupNodeLabels(registry)
