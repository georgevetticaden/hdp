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
							$log.log("inside method with httpResponse arg");
							$log.log(httpResponse)
							$scope.results = [];
							$scope.error = true;
							$scope.errorStatus = httpResponse.status;
							$scope.errorMessage = httpResponse.data;
						}
				);				
				
				
			};
			
			
			$scope.load();
			
		} ]);		
				
function ApplicationModel(stompClient, events, leafletDataContianer) {
	  var self = this;

	  self.username;
	  self.driverMontior = new DriverMonitorModel(leafletDataContianer);
	  self.notifications = [];
	  
	  self.truckSymbolSize;
	  
	  self.driverEvents = events;
	  
	  
	  self.connect = function() {
	    stompClient.connect('admin', 'admin', function(frame) {

	      console.log('Connected XXXX' + frame);
	      //self.username(frame.headers['user-name']);

	      //Update page with any new dangerous event that came in
	      //stompClient.subscribe("/topic/driver_infraction_events", function(message) {
	      stompClient.subscribe("/topic/driver_infraction_events", function(message) {
	           self.driverMontior.processDangerousEvent(JSON.parse(message.body));
	       });
	      
	      stompClient.subscribe("/topic/driver_events", function(message) {
	    	  //console.log(message);
	    	  self.driverMontior.renderOnMap(JSON.parse(message.body), self.truckSymbolSize);
	    	  //setTimeout(self.driverMontior().renderOnMap(JSON.parse(message.body), self.truckSymbolSize), 2000);
	      });      
	      
	      //Update page with any new alerts
	      stompClient.subscribe("/topic/driver_alert_notifications", function(message) {
	          self.pushNotification(JSON.parse(message.body).alertNotification);
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
		  console.log("loadDangerouseEvents..");
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
		  console.log("inside renderOnMap with event: " + driverEvent );
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
		    
		    console.log("inside renderNewDriverOnMap");
		    leafletData.getMap('map').then(function(map) {
		    	console.log("inside promiese......");
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