import sys;
import subprocess
  
sys.path.insert(0, "/mnt/environment/prod")


from setup.common import AmbariUtils

from setup.labels.main_setupNodeLabels import updateYarnConfigForLabels
from setup.queue.main_updateCSQueue import updateCapacityScheduler

def masterSetup():
    registry = AmbariUtils.createRegistry("config.properties");
    
    #do initialization required for labels
    subprocess.call(["labels/nodeLabelsSetup.sh"], shell=True)
    
    
    updateYarnConfigForLabels(registry)

    # Now Create the node labels and assign it to nodes
    subprocess.call(["labels/nodeLabelCreationAndAssignment.sh"], shell=True)
    
        
    #update capacity scheduler configs and refresh queues
    capacitySchedulerConfigFile = "../cluster/capacity-scheduler/csConfig-desired.json"    
    updateCapacityScheduler(registry, capacitySchedulerConfigFile);
    
    
    
masterSetup();