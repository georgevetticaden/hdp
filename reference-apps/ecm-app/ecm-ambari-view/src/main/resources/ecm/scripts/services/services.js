'use strict';

/* Services */

// Demonstrate how to register services
// In this case it is a simple value service.
angular.module('mvcRestBaseApp.services', [ 'resource' ]).value('version', '0.1');

var app = angular.module('mvcRestBaseApp.services', [ 'ngResource' ]);


app.factory('DocumentManagentService', [ '$resource', function($resource) {
	
	var service = {};
	
	service.search = $resource('/api/v1/views/ECM/versions/1.0.0/instances/ecm/resources/ecmservice/search/:queryStringParam1/:filterParam2/:startPageParam3/:facetFieldParam4/:highlightParam5', {}, {
		execute : {
			method : 'GET',
			params : {
				core : 'core',
				query : 'query'
			},
			isArray : false
		}
	
	
	});
	
	service.upload = $resource('/api/v1/views/ECM/versions/1.0.0/instances/ecm/resources/ecmservice/upload', {}, {
		execute : {
			method : 'POST',
			isArray : false,
			headers: {'Content-Type': 'multipart/form-data'}
		}
	
	
	});	
	
	return service;
	
} ]);


/*
app.factory('Hbase', [ '$resource', function($resource) {
	return $resource('download?:docId', {}, {
		download : {
			method : 'GET',
			params : {
				core : 'core',
				query : 'query'
			},
			isArray : false
		}
	
	
	});
} ]);

*/

/*
app.factory('Hbase', [ '$resource', function($resource) {
	var service = {};
	service.downloadsGetter = $resource('download?:docId', {}, {
		downloads : {
			method : 'GET',
			params : {
				core : 'core',
				query : 'query'
			},
			isArray : false
		}
	})

	return service;
} ]);

*/
app.config(function($httpProvider) {
	$httpProvider.responseInterceptors.push('myHttpInterceptor');

	var spinnerFunction = function spinnerFunction(data, headersGetter) {
		$("#spinner").show();
		return data;
	};

	$httpProvider.defaults.transformRequest.push(spinnerFunction);
});

app.factory('myHttpInterceptor', function($q, $window) {
	return function(promise) {
		return promise.then(function(response) {
			$("#spinner").hide();
			return response;
		}, function(response) {
			$("#spinner").hide();
			return $q.reject(response);
		});
	};
});