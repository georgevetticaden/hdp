/**
 * @ngdoc overview
 * @name mvcRestBaseApp
 * @requires ngRoute
 * @requires ngResource
 * @description
 * Creates an Angular application.
 * @author George Vetticaden
 */
angular.module('mvcRestBaseApp', [
  'ngRoute',
  'ngResource',
  'ngSanitize',
  'angular-websocket',
  'mvcRestBaseApp.services',
  'mvcRestBaseApp.filters',
  'ui.bootstrap',
  'angularFileUpload'
]).config(function($routeProvider) {
    $routeProvider.when('/welcome', {templateUrl: 'ecm/views/welcome.html', controller: 'WelcomeCtrl'});
    $routeProvider.when('/docs/search', {templateUrl: 'ecm/views/search.html', controller: 'DocSearchCtrl'});  
    $routeProvider.otherwise({redirectTo: '/welcome'})
}).config(function(WebSocketProvider){
    WebSocketProvider
    .prefix('')
    .uri('ws://localhost:9091/');
});
	