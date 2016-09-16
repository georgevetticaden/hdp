angular.module('mvcRestBaseApp')

  /**
   * @ngdoc controller
   * @name mvcRestBaseApp.controllers:WelcomeCtrl
   * @scope
   * @requires $scope
   * @description
   * Home page controller
   * @author George Vetticaden
   */
	.controller('WelcomeCtrl',
		[ '$scope', '$log', '$websocket', function($scope, $log, $websocket) {
			$log.log("*********inside welcome Ctrl********");
		} ])
		


  /**
   * @ngdoc TruckMonitorController
   * @name mvcRestBaseApp.controllers:TruckMonitorCtrl
   * @scope
   * @requires $scope
   * @description
   * Controller to Populate the Truck Events Data
   * @author George Vetticaden
   */
	.controller('TruckMonitorCtrl',
		[ '$scope', '$log', 'TruckMonitorService', 'leafletData',  function($scope, $log, TruckMonitorService, leafletData) {
		
			$scope.leafletDataContianer = leafletData;
			
			/* Load the Truck Events on Page load */
			$scope.load = function() {	
				
				// INitialize the map with the defaults and center definitions below. This configures how the map looks like
				$scope.defaults =  {
				        tileLayer: "http://{s}.tile.osm.org/{z}/{x}/{y}.png",
				        maxZoom: 14,
				        path: {
				            weight: 10,
				            color: '#800000',
				            opacity: 1
				        }
				    }	
					
				$scope.center = {
				        lat: 38.523884,
				        lng: -92.159845,
				        zoom: 6
			    }		
				
				
				
				// Call the TruckMonitor service to fetch all the existing truck events
				TruckMonitorService.search.execute(
						{}, 
						function (value, responseHeaders) {
							
							// NOw that we have all the existing events, initialize the socket connection and pass the existing events
							var socket = new SockJS('monitor');
			      	        var stompClient = Stomp.over(socket);
			      	        
			      	        var appModel = new ApplicationModel(stompClient, value.driverEventsResponse, $scope.leafletDataContianer);
					    
					        // Update the events tables with the new resutls
					        $scope.results = appModel;
					        
					        //Now Create the socket connection
					        appModel.connect();
					        

					
						},
						function(httpResponse) {
							$log.log(httpResponse)
							$scope.results = [];
							$scope.error = true;
							$scope.errorStatus = httpResponse.status;
							$scope.errorMessage = httpResponse.data;
						}
				);				
				
				
			};
			
			/* Generate the Truck Event Streams */
			$scope.generateTruckStreams = function() {
				
				$log.log("Inside TruckMonitorCtrl.generateTruckStreams..");
				//Reset the date of last event to current time since we started the stream
				 $scope.results.dateTimeOfLastEvent = new Date();
				TruckMonitorService.generateTruckStreams.execute( 
						
				);
						
			};
			
			/* Determines if events are flowing in. If so return true so Generate STreams button is disabled */
			$scope.isEventsFlowing = function() {
				
				if($scope.results != null) {
					var timeOfLastEvent = $scope.results.dateTimeOfLastEvent;
					
					var currentTime = new Date();
					var timeOfLastEventInMilliSeconds = currentTime.getTime() - timeOfLastEvent.getTime();
					
					// If the last event was more than 5 seconds ago, then assume no events are coming in and you can enable generate streams button
					if(timeOfLastEventInMilliSeconds > 5000) {
						return false;
					} else {
						return true;
					}
				} else {
					return true;
				}
				
						
			};			
			
			/** Initialize the page by calling load */
			$scope.load();
			
			
			
			
		} ])
		
		
  /**
   * @ngdoc ServiceRegistryCtrl
   * @name mvcRestBaseApp.controllers:ServiceRegistryCtrl
   * @scope
   * @requires $scope
   * @description
   * Controller configure the endpoints for the Application
   * @author George Vetticaden
   */
	.controller('ServiceRegistryCtrl',
		[ '$scope', '$log', 'TruckMonitorService',   function($scope, $log, TruckMonitorService) {
			
			
			

				
			$scope.initModel = function() {
				$scope.registryParams = {};
					
				$scope.registryParams.ambariUrl = "http://hdp0.field.hortonworks.com:8080"
				$scope.registryParams.clusterName="HDP_2_5";
				
				$scope.registryParams.hbaseSliderPublisherUrl="";
				$scope.registryParams.hbaseDeploymentMode="STANDALONE";
				
				$scope.registryParams.stormSliderPublisherUrl="";
				$scope.registryParams.stormDeploymentMode="STANDALONE";
				
				$scope.registryParams.activeMQHost ="streamanalyticsweb0.field.hortonworks.com";
				$scope.registryParams.solrServerUrl="http://george-search01.cloud.hortonworks.com:8983/solr";
			};
			
			$scope.configureEndpoints = function(registryParams) {
				$log.info("Inside ConfigureEndpoints, registryParams is: " + registryParams);
				$log.info("AmbariUrl is  is: " + registryParams.ambariUrl);
				$scope.registryParams = registryParams
				TruckMonitorService.configureEndpoints.execute(
						{
							'ambariUrl' : $scope.registryParams.ambariUrl,
							'clusterName' : $scope.registryParams.clusterName,
							'hbaseDeploymentMode' : $scope.registryParams.hbaseDeploymentMode,
							'hbaseSliderPublisherUrl': $scope.registryParams.hbaseSliderPublisherUrl,
							'stormDeploymentMode' : $scope.registryParams.stormDeploymentMode,
							'stormSliderPublisherUrl' : $scope.registryParams.stormSliderPublisherUrl,
							'activeMQHost' : $scope.registryParams.activeMQHost,
							'solrServerUrl' : $scope.registryParams.solrServerUrl
							
						}, 
						function (value, responseHeaders) {
							
							$log.info("Value returned is " + value)
					        

					
						},
						function(httpResponse) {
							$log.log(httpResponse)
							$scope.results = [];
							$scope.error = true;
							$scope.errorStatus = httpResponse.status;
							$scope.errorMessage = httpResponse.data;
						}
				);				
			};
			
			$scope.deployStormTopology = function() {
				$log.log("Inside deployStorm Topology...");
				TruckMonitorService.deployStormTopology.execute();				
			};
			
			$scope.initModel();
			
		}]);
				

function ApplicationModel(stompClient, events, leafletDataContianer) {
	  var self = this;

	  self.username;
	  self.driverMontior = new DriverMonitorModel(leafletDataContianer);
	  self.notifications = [];
	  self.alerts = new AlertModel();
	  
	  self.truckSymbolSize;
	  
	  self.driverEvents = events;
	  
	  self.dateTimeOfLastEvent = new Date();
	  
	  
	  self.connect = function() {
	    stompClient.connect('admin', 'admin', function(frame) {

	      console.log('Connected XXXX' + frame);

	      stompClient.subscribe("/topic/driver_infraction_events", function(message) {
	    	   //console.log("Dangerous Event came in..");
	           self.driverMontior.processDangerousEvent(JSON.parse(message.body));
	           self.dateTimeOfLastEvent = new Date();
	       });
	      
	      stompClient.subscribe("/topic/driver_events", function(message) {
	    	  //console.log(message);
	    	  self.driverMontior.renderOnMap(JSON.parse(message.body), self.truckSymbolSize);
	    	  self.dateTimeOfLastEvent = new Date();
	      });      
	      
	      //Update page with any new alerts
	      /* 
	      stompClient.subscribe("/topic/driver_alert_notifications", function(message) {
	          self.pushNotification(JSON.parse(message.body).alertNotification);
	        });
	        */
	      
	      stompClient.subscribe("/topic/driver_alert_notifications", function(message) {
	    	  console.log(message);
	          self.alerts.processAlert(JSON.parse(message.body));
	        });	      
	      
	    }, function(error) {
	      console.log("STOMP protocol error " + error);
	    });
	  }

	  self.pushNotification = function(text) {
	    self.notifications.push({notification: text});
	    if (self.notifications().length > 5) {
	      self.notifications.shift();
	    }
	  }

	  self.logout = function() {
	    stompClient.disconnect();
	    window.location.href = "../logout.html";
	  }
	  

	  // Loads all the dangerous events for all drivers on page load
	  self.loadEvents = function(driverEvents) {	  
		var lat = driverEvents.startLat;
		var long = driverEvents.startLong;
		var zoomLevel = driverEvents.zoomLevel;

		self.truckSymbolSize=driverEvents.truckSymbolSize;
		console.log("truckSymbolSize is: " + self.truckSymbolSize);
		console.log("self.driverMonitor is: " + self.driverMontior);
		
		
	    self.driverMontior.loadDangerousEvents(driverEvents.violationEvents);
	    self.driverMontior.initializeMap(lat, long, zoomLevel);
	  };  
	  
	  self.loadEvents(self.driverEvents);
	  
	}

	function DriverMonitorModel(leafletDataContianer) {
	  
		var self = this;
	 
		self.rows = [];

		var leafletData = leafletDataContianer;

		var rowLookup = {};
	  
		var driverOnMapLookup = {};
		var stopIncreasingRadius = false;
	  

	  
	  self.initializeMap = function(lat, long, zoomLevel ) {
		  console.log("inside initialize mapp...");
		  console.log("log:" + long)
		  console.log("lat:" + lat);	
		  console.log("zoomlevel:" + zoomLevel);  
		  
		  //for now don't do anything
	  }
	  
	  
	  self.loadDangerousEvents = function(positions) {
		  for ( var i = 0; i < positions.length; i++) {
	    	
	    	self.loadDangerousEvent(positions[i]);
	    }
	  };
	  
	  self.loadDangerousEvent = function (position) {
	  	var row = new DriverRow(position);
		self.rows.push(row);
		rowLookup[row.driverId] = row;	  
	  }
	  
	  self.processDangerousEvent = function(driverEvent) {
		 	if (rowLookup.hasOwnProperty(driverEvent.driverId)) {
		 		rowLookup[driverEvent.driverId].highlight();
		 		rowLookup[driverEvent.driverId].updateEvent(driverEvent);
		 		setTimeout(function() {
		 			
		 			rowLookup[driverEvent.driverId].unHighlight();
		 			
		 		}, 2000);
		 		
		 		
		    } else {
		    	self.loadDangerousEvent(driverEvent);
		    }
		  }; 
		  
		  

	  self.renderOnMap = function(driverEvent, truckSymbolSize) {
		  if (driverOnMapLookup.hasOwnProperty(driverEvent.driverId)) {
			  var driverOnMap = driverOnMapLookup[driverEvent.driverId].driverOnMap;
			  var previousDriverEvent = driverOnMapLookup[driverEvent.driverId].driverEvent;
			  
			  driverOnMap.setLatLng([driverEvent.latitude, driverEvent.longitude]);
			  
			  var driverMsg;
			  var alert = driverOnMap.getRadius() > 40000;
			  if(driverEvent.numberOfInfractions == previousDriverEvent.numberOfInfractions) {
				  driverMsg = self.constructMessage(driverEvent.driverId, driverEvent.numberOfInfractions, previousDriverEvent.infractionEvent, driverEvent.driverName, driverEvent.routeId, driverEvent.routeName, alert);
			  
			  } else {
				  driverMsg = self.constructMessage(driverEvent.driverId, driverEvent.numberOfInfractions, driverEvent.infractionEvent, driverEvent.driverName, driverEvent.routeId, driverEvent.routeName, alert);
			  }
			  
			  driverOnMapLookup[driverEvent.driverId].driverEvent = driverEvent;
			  
			  //driverOnMap.bindPopup(driverMsg);
			  if(driverEvent.infractionEvent != 'Normal') {
				  driverOnMap.closePopup();
				  
				  
				  if(driverOnMap.getRadius() > 40000) {	
					  //console.log("Raidus is either greater than 40000 or all radisus increases have stopped " + newRadius);
					  stopIncreasingRadius = true; 				    			  
				  }
				  if(!stopIncreasingRadius) {
					  
					  var newRadius = driverOnMap.getRadius() * 1.1;
					  driverOnMap.setRadius(newRadius);
				  }
				  
				  //console.log("New Radius is: " + newRadius);
				  
				  driverOnMap.openPopup();
				  driverOnMap.bindPopup(driverMsg);
			  } else {
				  if(driverOnMap._popup._isOpen) {
					  driverOnMap.closePopup();
					  driverOnMap.bindPopup(driverMsg);
					  driverOnMap.openPopup();
				  }
				  
			  }
			  
				 
		  } else {
			  self.renderNewDriverOnMap(driverEvent, truckSymbolSize);
		  }
	  }
	  

	  
	  self.renderNewDriverOnMap = function (driverEvent, truckSymbolSize) {
		    var randomColor = '#' + (Math.random() * 0xFFFFFF << 0).toString(16);
		    
		    leafletData.getMap('map').then(function(map) {
		    	var driverOnMap = L.circle([driverEvent.latitude, driverEvent.longitude], truckSymbolSize, {
			        color: randomColor,
			        fillColor: randomColor,
			        fillOpacity: 0.8
			    }).addTo(map);   
			  	
				var driverMsg = self.constructMessage(driverEvent.driverId, driverEvent.numberOfInfractions, driverEvent.infractionEvent, driverEvent.driverName, driverEvent.routeId, driverEvent.routeName, false);
			  	driverOnMap.bindPopup(driverMsg);
			  	var driverDetails = {driverEvent:driverEvent, driverOnMap:driverOnMap};
			  	driverOnMapLookup[driverEvent.driverId] = driverDetails;	  
            });
		    
		  	
		    
	  
	  }; 
	  
	  self.constructMessage = function(driverId, numberOfInfractions, lastViolation, driverName, routeId, routeName, alertDriver) {
		  
		  var coreMessage = 	  	
		    "<b>Driver Name: </b> " + driverName +
		  	"</br>" + 
		  	"<b>Route Name: </b> " + routeName +
		  	"</br>" +  
		    "<b>Violation Count: </b>" + numberOfInfractions +
		    "</br>" +
		    "<b>Last Violation: </b>" + lastViolation +
		    "</br>";
		  
		  if(alertDriver) {
			  //console.log("alertDriver is true");
			  var alertMsg = "<b><h5>CONTACT DRIVER IMMEDIATELY</h5></b>";
			  coreMessage = coreMessage + alertMsg;
		  }
		  
		  var message= " <div> "+ coreMessage  + "</div>";
		  return message;
		};

	};
	
	
	
	function AlertModel() {
		var self = this;
		self.rows = [];
		var rowLookup = {};
		
		  self.processAlert = function(alertEvent) {
			  console.log(alertEvent)
			 	if (rowLookup.hasOwnProperty(alertEvent.infractionDetail.truckDriver.driverId)) {
			 		rowLookup[alertEvent.infractionDetail.truckDriver.driverId].updateEvent(driverEvent);	 		
			 		
			    } else {
			    	self.loadAlert(alertEvent);
			    }
			  }; 	
			  
			  self.loadAlert = function (position) {
				  	var row = new AlertRow(position);
					self.rows.push(row);
					rowLookup[row.driverId] = row;	  
				  }		  
	}	


	function DriverRow(data) {
		
	  var self = this;

	  self.truckDriverEventKey = data.truckDriverEventKey;
	  self.driverId = data.driverId;
	  self.driverName = data.driverName;

	  self.timeStampString = data.timeStampString;
	  self.longitude = data.longitude;
	  self.latitude = data.latitude;
	  self.infractionEvent = data.infractionEvent;
	  self.truckId = data.truckId;
	  self.routeId = data.routeId;
	  self.routeName = data.routeName;
	  self.numberOfInfractions = data.numberOfInfractions;
	  self.rowClass="";
	  
	  self.updateEvent = function(driverEvent) {
		  	
		    self.timeStampString = driverEvent.timeStampString;
		    self.longitude = driverEvent.longitude;
		    self.latitude = driverEvent.latitude;
		    self.infractionEvent = driverEvent.infractionEvent;
		    self.numberOfInfractions = driverEvent.numberOfInfractions;
		    self.routeId = driverEvent.routeId;
		    self.truckId = driverEvent.truckId;
		    self.routeName = driverEvent.routeName;

	  };  
	  
	  self.highlight = function() {
		  self.rowClass="highlight";
	  };
	 
	  self.unHighlight = function() {
		  self.rowClass="";
	  };  

	};
	
	function AlertRow(data) {
		  var self = this;

		  self.notificationTimestamp = data.notificationTimestamp;
		  self.notificationMessage = data.notificationMessage;
		  self.alertType = data.alertName;
		  self.driverName = data.infractionDetail.truckDriver.driverName;
		  self.driverId = data.infractionDetail.truckDriver.driverId;
		  
		  self.updateEvent = function(driverEvent) {
			  	
			   self.notificationTimestamp(driverEvent.notificationTimestamp);
			   self.notificationMessage(driverEvent.notificationMessage);
			   self.alertType(driverEvent.alertName);

		  };  
		
		};
	