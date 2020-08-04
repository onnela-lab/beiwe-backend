(function(){
    angular
    .module('surveyBuilder')
    .controller('FilterableListController', ['$scope', '$window', function($scope, $window) {
        $scope.filterableObjects = $window.filterableObjects;
        $scope.filterText = ''
    }]);
}());
