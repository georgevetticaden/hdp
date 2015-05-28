from subprocess import call
import sys;
import logging
import time

# Replace the below to ensure to right the path
sys.path.insert(0, "[REPLACE]/hdp/reference-apps/iot-trucking-app/trucking-env-setup/environment/prod")

from setup.common import AmbariUtils


def saveNewCapacitySchedulerConfig(registry, capacitySchedulerConfigFile):
    logging.info("Saving New Capacity Scheduler Config");
    schedulerConf = open(capacitySchedulerConfigFile, 'r')
    schedulerData = schedulerConf.read()
    schedulerConf.close()
    schedulerData=str.replace(schedulerData,'version1400618703537','version'+str(int(time.time()*1000)))
    logging.debug(schedulerData)
    AmbariUtils.restPUT(registry, registry['clusterName'],schedulerData)


def updateCapacityScheduler(registry, capacitySchedulerConfigFile):

    print("Saving New Capacity Schduler Config")
    saveNewCapacitySchedulerConfig(registry, capacitySchedulerConfigFile)

    print("Refreshing Queues with the Changes")
    AmbariUtils.refreshQueues(registry)

registry = AmbariUtils.createRegistry("../config.properties");
capacitySchedulerConfigFile = "../../cluster/capacity-scheduler/csConfig-desired.json"
updateCapacityScheduler(registry, capacitySchedulerConfigFile)