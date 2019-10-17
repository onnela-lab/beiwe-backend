/*
 * js document used to color the background of cells for the participant dashboard
 * identifies the max and min per ROW and highlights based on that gradient
 */
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
        $scope.calculateColor = calculateColor;
        $scope.setUp = setUp;
        $scope.createUrl = createUrl;
        $scope.createNewUrl = createNewUrl;
        $scope.evalColorRange = evalColorRange;
        $scope.removeColor = removeColor;
        $scope.addFlag = addFlag;
        $scope.removeFlag = removeFlag;
        $scope.checkFlag = checkFlag;
        $scope.flagExists = flagExists;
        $scope.createDateRangeUrl = createDateRangeUrl;
        $scope.addGradient = addGradient;
        $scope.getCurrentGradient = getCurrentGradient;
        $scope.valueIsNumber = valueIsNumber;
        $scope.convertToNumber = convertToNumber;
        $scope.flag_operator = null;
        $scope.flag_value = null;
        $scope.current_gradient = $window.current_gradient;
        $scope.color_high_range = $window.color_high_range;
        $scope.color_low_range = $window.color_low_range;
        $scope.show_color = $window.show_color;
        $scope.all_flags_list = $window.all_flags_list;
        $scope.print_flags_dict = $window.print_flags_dict;
        $scope.base_past_url = $window.base_past_url;
        $scope.base_next_url = $window.base_next_url;
        $scope.start_date = $window.start_date;
        $scope.end_date = $window.end_date;
        setUp();

        // ------------------------ FUNCTIONS ----------------------- //
        //NOTE: this checking is necessary DESPITE the fact that type=number is specified in the input fields
        // BECAUSE some browsers do not support type=number and will allow any characters.
        //converts a number with commas in it to a regular number
        // if any other non numerical character is found it returns False
        function convertToNumber(number){
            number = number.toString();
            let new_num = number.replace(/[^0-9]/,'');
            if(new_num !== ''){
                    return new_num;
                }
            else{
                return false;
            }
        }

        //returns true if number is normal or has commas in it, false in all other circumstances
        function valueIsNumber(number){
            if(number === null || number === undefined) {
                return false;
            }
            else if(convertToNumber(number) === false){
                return false;
            }
            return true;
        }

        function getCurrentGradient(gradient_value){
            if(gradient_value === null){
                return 0;
            }
            return gradient_value;
        }

        function addGradient() {
            $scope.show_color = true;
            let color_low = convertToNumber($scope.color_low_range);
            let color_high = convertToNumber($scope.color_high_range);
            $scope.current_gradient = [color_low, color_high];
            $scope.color_low_range = null;
            $scope.color_high_range = null;
        }

        function createDateRangeUrl(){
            var start_date = $('#start_datetimepicker').data('date');
            var end_date = $('#end_datetimepicker').data('date');
            const base_url = "?&start="+start_date+"&end="+end_date;
            let str = "";
            for(let flag_arr of $scope.all_flags_list){
                str += flag_arr[0]+","+flag_arr[1]+"*";
            }

            const flags = str.substr(0, str.length-1);
            $window.location = (base_url+"&color_low="+$scope.current_gradient[0]+"&color_high="+$scope.current_gradient[1]+"&show_color="+$scope.show_color+"&flags="+flags);
        }

        // check if flag is already present
        function flagExists(){
            if ($scope.all_flags_list !== [] && $scope.flag_operator !== null && $scope.flag_value !== null) {
                for (let flag_arr of $scope.all_flags_list) {
                    if (flag_arr[0] === $scope.flag_operator && $scope.flag_value === flag_arr[1]) {
                        return 1; // this shows the error message that the flag already exists
                    }
                }
                return 2; //this shows the add flag button bc they have entered things and it does not currently exist
            }
            // this shows nothing because they haven't entered anything
            return 3;
        }

        //remove flag that got clicked
        function removeFlag(flag){
            for(let i = 0; i < ($scope.all_flags_list).length; i++){
                if(($scope.all_flags_list[i][0]) === flag[0] && ($scope.all_flags_list[i][1]) === flag[1]){
                    $scope.all_flags_list.splice(i, 1);
                }
            }
        }

        //check if the flag is present
        function checkFlag(flag){
            return all_flags_list.includes([flag[0], flag[1]]);
        }

        //add flag
        function addFlag(){
            number = convertToNumber($scope.flag_value);
            $scope.all_flags_list.push([$scope.flag_operator, number]);
            $scope.flag_operator = null;
            $scope.flag_value = null;
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

        //remove color if widget is unclicked
        function removeColor(){
            $scope.show_color=false;
            $scope.current_gradient = [null, null];
        }

        // evaluate color range to make sure min is smaller than max
        function evalColorRange(){
            if($scope.color_high_range !== null && $scope.color_low_range !== null)
                return !($scope.color_high_range > $scope.color_low_range);
            return false;

        }

        // function that originally sets up the colors before input
        function setUp() {
            if($window.current_gradient[0] === 0 && $window.current_gradient[1] === 0) {
                let max = null;
                let min = null;
                const table = $("#dashboard-datastream-table tbody");
                $("td", table).each(function(){
                    const num = Number($(this).attr("data-number"));
                    if (max === null) {
                        max = num;
                    }
                    if (min === null) {
                        min = num;
                    }
                    if (num > max){
                        max = num;
                    }
                    if (num < min){
                        min = num;
                    }
                });
                $scope.color_high_range = null;
                $scope.color_low_range = null;
                //this is a case that is very annoying. basically, if you are loading the page with no default color setting,
                // you want the gradient to default to the max and min on the page. however, if you are loading the
                // page WITH a default color setting but you DON'T want that to show, AND you choose to save the current
                // filter settings to the backend without changing the gradient info (which you have not touched),
                // it will save the default that you set on the page instead of the nothing gradient which you had originally
                // loaded from the default color settings model.
                if($scope.show_color === true) {
                    $scope.current_gradient = [min, max]
                }
                else{
                    $scope.current_gradient = [null, null]
                }
            }
        }

        //update base url to include max and min endpoints
        function createUrl(base_url) {
            return (base_url + "&color_low=" + $scope.color_low_range + "&color_high=" + $scope.color_high_range)
        }

        // calculate new color scheme when user changes the values
        function calculateColor(value) {
            let amount_gradient = 0;
            let flag = false;
            if($scope.all_flags_list !== []){
                for(let flag_arr of $scope.all_flags_list){
                    if(flag_arr[0] === ">"){
                        if(flag_arr[1] < value){
                            flag = true;
                        }
                    }
                    else if(flag_arr[0] === "="){
                        if(flag_arr[1] === value){
                            flag = true;
                        }
                    }
                    else if(flag_arr[0] === "<"){
                        if(flag_arr[1] > value){
                            flag = true;
                        }
                    }
                }
            }

            if($scope.show_color && $scope.current_gradient[0] < $scope.current_gradient[1]) {
                const max = $scope.current_gradient[1];
                const min = $scope.current_gradient[0];
                const adjusted_max = max - min;
                value -= min;
                amount_gradient = value / adjusted_max;
            }

            if(flag === true){
                return{"border": "solid", "background-color": `rgba(67, 170, 54, ${amount_gradient}`};
            }
            else{
                return {"background-color": `rgba(67, 170, 54, ${amount_gradient}`};
            }
        }
    }]);
}());






