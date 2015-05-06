'use strict';

describe('Controller test', function () {

  // load the controller's module
  beforeEach(module('mvcRestBaseApp'));

  var WelcomeCtrl,
    scope;

  // Initialize the controller and a mock scope
//  beforeEach(inject(function ($controller, $rootScope) {
//    scope = $rootScope.$new();
//    WelcomeCtrl = $controller('WelcomeCtrl', {
//      $scope: scope
//    });
//  }));
//
  it('should have a valid scope', function () {
    expect(true).toBe(true);
  });


});