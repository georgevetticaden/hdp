import configparser
import logging
import json
import http.client
import base64
import time
import subprocess
import random



def createRegistry(configfile):
    
    config = configparser.ConfigParser()
    
    #Setup the config File
    config.read(configfile)
    
    #Setup Logging
    logging.basicConfig(filename=config['LOGGING']['file'],level=config['LOGGING']['level'])
    
    registry = {};
    registry = getAmbariConnectionInfo(config)
    registry['clusterName'] = getClusterName(registry);
    
    return registry
    
def getAmbariConnectionInfo(config):
    registry = {}
    registry['ambariUser']=config['AMBARI']['user'] 
    registry['ambariPass']=config['AMBARI']['password']
    registry['ambariPort']=config['AMBARI']['port']
    registry['ambariURL']=config['AMBARI']['url'].strip()
        
    
    #Auth Token
    registry['tok'] = (registry['ambariUser']+':'+registry['ambariPass']).strip()
    registry['authToken']=base64.b64encode(registry['tok'].encode('ascii'))
    registry['HEADERS'] = {'Authorization' : 'Basic '+(registry['authToken']).decode('ascii'), 'content-type' : 'text/plain', 'X-Requested-By':'ambari'}    
    
    return registry;
    
def getClusterName(registry):
    apiShort = '/api/v1/clusters/'
   
    logging.info('url for clusters reading ' + registry['ambariURL']+':'+registry['ambariPort'] + apiShort)
    r = restGET(registry, None)
    clusterName = (json.loads(r))['items'][0]['Clusters']['cluster_name']
    logging.info('elected cluster name=' + clusterName)
    return clusterName;

## Custom Methods
###############################
####TO DO, merge all REST Methods
def restGET(registry, uri):
    apiShort = '/api/v1/clusters/'
    
    req = http.client.HTTPConnection(registry['ambariURL'], registry['ambariPort'])
    
    if uri == None:
        req.request("GET", apiShort, None, registry['HEADERS'])
    else:
        req.request("GET", apiShort+uri, None, registry['HEADERS'])

    resData = req.getresponse().read().decode('utf-8')
    logging.debug(resData)
    return resData

def restPUT(registry, uri, jsonObj):
    apiShort = '/api/v1/clusters/'
    
    DATA = bytes(str(jsonObj).encode('utf-8'))
    
    conn = http.client.HTTPConnection(registry['ambariURL'], registry['ambariPort'])

    if uri == None:
        conn.request("PUT", apiShort, DATA, registry['HEADERS'])
    else:
        conn.request("PUT", apiShort+uri, DATA,registry['HEADERS'])

    res =conn.getresponse()
    print(res.status, res.reason)

def restPOST(registry, uri, jsonObj):
    apiShort = '/api/v1/clusters/'
    
    DATA = bytes(str(jsonObj).encode('utf-8'))
    
    conn = http.client.HTTPConnection(registry['ambariURL'], registry['ambariPort'])
    
    if uri == None:
        conn.request("POST", apiShort, DATA, registry['HEADERS'])
    else:
        conn.request("POST", apiShort+uri, DATA,registry['HEADERS'])
    
    res = conn.getresponse()
    print(res.status, res.reason)

###WARNING###
#Find the current version of the config so we can restore it...
# type is the type of config we want like 'yarn-site' or 'capacity-scheduler'
#this only works if people have not been making custom tag versions!
def vectVersion(jsonObj, type):

#Versions List 
    propsList = []
    tagsList = []

    jsonObj = json.loads(jsonObj)    
    logging.debug('inside vectVersion printing passed value = '+str(jsonObj))
    logging.debug('length of items array '+str(jsonObj['items']))

#collect all capsch version
    for prop in jsonObj['items']:
        if prop['type'] == type:
            propsList.append(prop)
            logging.debug('prop added to list ' +str(prop))
        
#get all tag versions
    for tag in propsList:
        tagsList.append(tag['tag'])
        
#find the greatest tag - Its this part that breaks if versions are not timestamp tags
    tagsList.sort()
    logging.debug('sorted tagsList = ' + str(tagsList))
    return tagsList[-1]


def stopYarnService(registry):  
    print('Stopping YARN Service on ' + registry['clusterName'])
    restPUT(registry, registry['clusterName']+'/services/YARN/', '{"RequestInfo": {"context": "Stop YARN via REST"},"ServiceInfo": {"state": "INSTALLED"}}')

def startYarnService(registry):
    print('Starting YARN Service')
    restPUT(registry, registry['clusterName']+'/services/YARN/', '{"RequestInfo": {"context": "Start YARN via REST"},"ServiceInfo": {"state": "STARTED"}}')

def getRMHost(registry):
    rmHost = (json.loads(restGET(registry, registry['clusterName']+'/services/YARN/components/RESOURCEMANAGER')))['host_components'][0]['HostRoles']['host_name']
    return rmHost

def refreshQueues(registry):
    logging.info("refreshing queues");
    rmHost = getRMHost(registry)
    refQueuesCmd = '{"RequestInfo" : {"command" : "REFRESHQUEUES","context" : "Refresh YARN Capacity Scheduler"},"Requests/resource_filters": [{"service_name" : "YARN","component_name" : "RESOURCEMANAGER","hosts" : "' + rmHost +'"}]}'
    restPOST(registry, registry['clusterName']+'/requests', refQueuesCmd)
    
    

