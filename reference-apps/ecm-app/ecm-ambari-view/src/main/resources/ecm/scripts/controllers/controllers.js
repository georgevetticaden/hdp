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
		[ '$scope', '$log', 'WebSocket', function($scope, $log, WebSocket) {
			// Register with the Websocket instance on the server to start
			// handling events
			WebSocket.onopen(function() {
				$log.log('connection');
				WebSocket.send('Test Message')
			});

			WebSocket.onmessage(function(event) {
				$log.log('Recieved Message: ', event.data);
				$('#alert_placeholder').html('<div class="alert alert-warning alert-dismissible" role="alert"><button type="button" class="close" data-dismiss="alert"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button><strong>404!</strong> - '+event.data+'</div>')
			});
		} ])
	.controller(
		'DocSearchCtrl',
		[
				'$scope',
				'$log',
				'DocumentManagentService',
			    '$modal',
				'$filter',
				function($scope, $log, DocumentManagentService, Hbase, $modal, $filter) {
					$log.log("Entered Search Controller")
					$scope.master = {};
					// Search Function
					$scope.search = function(query) {
						$log.log("Query is: " + query);
						$scope.master = query;
						// startPage and filter are dummied out..
						var startPage = 0;
						var filter = "1";
						var facetField = "last_author";
						var highlight = "body";
						var queryString = query.q;				
		
						
						DocumentManagentService.search.execute({
							core : "rawdocs",
							queryStringParam1: queryString,
							filterParam2: filter,
							startPageParam3: startPage,
							facetFieldParam4: facetField,
							highlightParam5: highlight
						}, function(value, responseHeaders) {
							$log.log("Value is: " + value);
							$log.log("Results is: " + value.results);
							$log.log("Highlights is: " + value.highlights);
							$log.log("Facets is: " + value.facets);
							$scope.results = value.results;
							$scope.totalItems = value.numFound;
							$scope.highlights = value.highlights;
							$scope.error = false;
							$scope.chart = {
								"type" : "PieChart",
								"data" : [ [ "Authors", "Count" ] ]
										.concat(value.facets["last_author"]),
								"options" : {
									"displayExactValues" : true,
									"width" : 400,
									"height" : 120,
									"is3D" : true,
									"chartArea" : {
										"left" : 10,
										"top" : 10,
										"bottom" : 0,
										"height" : "100%"
									}
								},
								"formatters" : {},
								"displayed" : true
							}
						}, function(httpResponse) {
							$log.log(httpResponse)
							$scope.results = [];
							$scope.error = true;
							$scope.errorStatus = httpResponse.status;
							$scope.errorMessage = httpResponse.data;
						});
					};
					
					// Pagination with a max page size of 25 pages in the
					// carousel
					$scope.maxSize = 25
					if ($scope.currentPage < 1) {
						$scope.currentPage = 1;
					}
					
					$scope.pageChanged = function() {
						console.log('Page changed to: ' + $scope.currentPage);
						$scope.search($scope.query)
					};
					
					$scope.open = function (docBody, highlights) {
				    var modalInstance = $modal.open({
				      templateUrl: 'docModalContent.html',
				      controller: 'ModalInstanceCtrl',
				      size: 'lg',
						resolve:  {
							docBody: function() {
								if (highlights && highlights[0]) {
									docBody = $filter('highlight-highlights')(docBody, highlights)
								}
								return $filter('nl2br')(docBody);
							}
						}
				    });

				    modalInstance.result.then(function (selectedItem) {
				      $scope.selected = selectedItem;
				    }, function () {
				    });
				  };
				  
				   $scope.updateModal = function(docBody) {
						$scope.docBody = docBody;
					}
				   
					$scope.download = function (docId) {
					    console.log("Dowlloadign doc wtih docId["+docId+"]");
					    var hbaseQuery = "docId=" + docId;
						
					    Hbase.download({
							core : "rawdocs",
							docId : hbaseQuery
						}, function(value, responseHeaders) {
							$log.log("Value from download is: " + value);
							
							$scope.error = false;
							
						}, function(httpResponse) {
							$log.log(httpResponse)
							$scope.results = [];
							$scope.error = true;
							$scope.errorStatus = httpResponse.status;
							$scope.errorMessage = httpResponse.data;
						});
					};					   
				   
				   
				   
				} ])
				
	.controller('ModalInstanceCtrl', function ($scope, $modalInstance, docBody) {
				  $scope.docBody = docBody;

				  $scope.close = function () {
				    $modalInstance.dismiss('Close');
				  }
				})
				
	.controller('DocUploadCtrl2', ['$scope', '$log', '$upload', 
	     function ($scope, $log,  $upload) {
		    $scope.$watch('files', function () {
		        //$scope.upload($scope.files);
		    });
		
		    $scope.upload = function (doc) {
		    	var files = doc.files;
		    	$log.log("inside upload of ctrl2, files size is: " + files);
		        if (files && files.length) {
		            for (var i = 0; i < files.length; i++) {
		                var file = files[i];
		                $upload.upload({
		                	url: '/api/v1/views/ECM/versions/1.0.0/instances/ecm/resources/ecmservice/upload',
		                    fields: {'documentName': doc.documentName, 'documentClass': doc.documentClass, 'customerName':doc.customerName},
		                    file: file,
		                    headers: {'X-Requested-By': 'ambari'}
		                }).progress(function (evt) {
		                    file.progress = parseInt(100.0 * evt.loaded / evt.total);
		                    console.log('progress: ' + file.progress + '% ' + evt.config.file.name);
		                }).success(function (data, status, headers, config) {
		                	console.log("inside success");
		                	file.result = data;
		                    //console.log('file ' + config.file.name + 'uploaded. Response: ' + fie.results);
		                });
		            }
		        }
		    };
		    
		    
}]);				
