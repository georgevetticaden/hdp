'use strict';

/* Services */

// Demonstrate how to register services
// In this case it is a simple value service.
angular.module('mvcRestBaseApp.services', [ 'resource' ]).value('version', '0.1');

var app = angular.module('mvcRestBaseApp.services', [ 'ngResource' ]);


app.factory('TruckMonitorService', [ '$resource', function($resource) {
	
	var service = {};
	
	service.search = $resource('iotdemo/truck/monitor/driverEvents.json', {}, {
		execute : {
			method : 'GET',
			isArray : false
		}
	
	});
	
	service.generateTruckStreams = $resource('iotdemo/truck/monitor/streaming', {}, {
		execute : {
			method : 'POST',
			isArray : false
		}
	
	});	

	service.configureEndpoints = $resource('iotdemo/truck/install/configureEndpoints', {}, {
		execute : {
			method : 'POST',
			isArray : false
		}
	
	});	
	
	service.deployStormTopology = $resource('iotdemo/truck/install/deployStormTopology', {}, {
		execute : {
			method : 'POST',
			isArray : false
		}
	
	});		
	
	
	

	
	return service;
	
} ]);


