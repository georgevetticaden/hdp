/**
 * @ngdoc overview
 * @name mvcRestBaseApp
 * @requires ngRoute
 * @requires ngResource
 * @description
 * Creates an Angular application.
 * @author İsmail Demirbilek
 */
angular.module('mvcRestBaseApp', [
  'ngRoute',
  'ngResource',
  'angular-websocket',
  'mvcRestBaseApp.services',
  'leaflet-directive'
]).config(function($routeProvider) {
	$routeProvider.when('/welcome', {templateUrl: 'views/welcome.html', controller: 'WelcomeCtrl'}); 
	$routeProvider.when('/monitorTrucks', {templateUrl: 'views/monitorTrucks.html', controller: 'TruckMonitorCtrl'}); 
	$routeProvider.when('/login', {templateUrl: 'views/login.html', controller: 'LoginCtrl'}); 
	$routeProvider.when('/configureHDPServiceEndpoints', {templateUrl: 'views/serviceEndpoints.html', controller: 'ServiceRegistryCtrl'}); 
	$routeProvider.otherwise({redirectTo: '/welcome'})
});

	



