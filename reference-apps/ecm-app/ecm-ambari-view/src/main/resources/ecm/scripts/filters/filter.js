'use strict';

/* Filters */

angular.module('mvcRestBaseApp.filters', []).
  filter('interpolate', ['version', function(version) {
    return function(text) {
      return String(text).replace(/\%VERSION\%/mg, version);
    };
  }]).filter('nl2br', ['$sce', function ($sce) {
    return function (text) {
        return text ? $sce.trustAsHtml(text.replace(/\n/g, '<br/>')) : '';
    };
}]).filter('highlight-highlights', ['$sce', function ($sce) {
    return function (text, highlights) {
		var highlightedCleanedText = text;
		highlights = highlights[0];
		for (var i = 0; i < highlights.length; i++) {
			var sanitizedHighlight = highlights[i].replace(/<em>/g, "").replace(/<\/em>/g, "");
			highlightedCleanedText = highlightedCleanedText.replace(new RegExp(sanitizedHighlight.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1")), highlights[i]);
		}
		return highlightedCleanedText;
    };
}]).filter('convertJavaDate', ['$sce', function ($sce) {
    return function (date) {
		var d = new Date(date);
        return d;
    };
}]);