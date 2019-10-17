$(document).ready(function() {
    $('#dashboard-datastream-table').DataTable();
    $('#start_datetimepicker').datetimepicker({
        format: "YYYY-MM-DD",
        defaultDate: window.start_date,
        minDate: window.first_day,
        maxDate:  window.last_day,
        useCurrent: false,
    });
    $('#end_datetimepicker').datetimepicker({
        format: "YYYY-MM-DD",
        defaultDate: window.end_date,
        minDate: window.first_day,
        maxDate: window.last_day,
        useCurrent: false,
    //    you need use current because of a bug in the bootstrap datetimepicker. if you don't have it set to false,
    //    the default date will get overridden to be the max date, and in the absence of a max date will set to the
    //    current date.
    });
});

//main function that runs everything
(function(){
    angular
    .module("surveyBuilder")
    .controller('buttonController', ['$scope', '$window', function($scope, $window) {

        // change when they change the input
        $scope.createNewUrl = createNewUrl;
        $scope.createPatientDateRangeUrl = createPatientDateRangeUrl;
        $scope.base_past_url = $window.base_past_url;
        $scope.base_next_url = $window.base_next_url;
        $scope.start_date = $window.start_date;
        $scope.end_date = $window.end_date;

        // ------------------------ FUNCTIONS ----------------------- //
        function createPatientDateRangeUrl(){
            var start_date = $('#start_datetimepicker').data('date');
            var end_date = $('#end_datetimepicker').data('date');
            const base_url = "?&start="+start_date+"&end="+end_date;
            $window.location = base_url;
        }

        //create a new url for the next and past buttons
        function createNewUrl(base_url){
            let str = "";
            for(let flag_arr of $scope.all_flags_list){
                str += flag_arr[0]+","+flag_arr[1]+"*";
            }
            const flags = str.substr(0, str.length-1);
            $window.location = (base_url+"&color_low="+$scope.current_gradient[0]+"&color_high="+$scope.current_gradient[1]+"&show_color="+$scope.show_color+"&flags="+flags);
        }

    }]);
}());