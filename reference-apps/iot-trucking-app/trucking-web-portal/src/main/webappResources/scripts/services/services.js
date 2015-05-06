'use strict';

/* Services */

// Demonstrate how to register services
// In this case it is a simple value service.
angular.module('mvcRestBaseApp.services', [ 'resource' ]).value('version', '0.1');

var app = angular.module('mvcRestBaseApp.services', [ 'ngResource' ]);


app.factory('TruckMonitorService', [ '$resource', function($resource) {
	
	var service = {};
	
	service.search = $resource('storm/truckdemo/driverEvents.json', {}, {
		execute : {
			method : 'GET',
			isArray : false
		}
	
	});
	

	
	return service;
	
} ]);


