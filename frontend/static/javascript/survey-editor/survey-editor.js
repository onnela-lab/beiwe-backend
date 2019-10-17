/**
 * Functionality to create and edit surveys via a web form
 */

$(document).ready(function() {
    window.scope = angular.element($("body")).scope();
    renderSchedule();
    $('.schedule-timepicker').timepicker();
    audioSurveyTypeChange( $("[name='audio_survey_type']:checked").val() )
});


// Return the hour number (in 24-hour time) that the user selected in the form
function getHour() {
    var hour = parseInt(document.getElementById("hour").value);
    var ampm = document.getElementById("ampm").value;
    if (hour == 12) {
        hour = 0; // 12a.m. is really 0:00; 12p.m. is really 12:00
    }
    if (ampm.localeCompare("pm") == 0) {
        hour += 12; // If time is p.m., add 12 to the hour
    };
    return hour;
}

// If it's a weekly survey, return the selected weekday (Sunday = 1, Saturday = 7)
function getDayOfWeek() {
    var dayPicker = document.getElementById("day");
    if (dayPicker === 'undefined' || dayPicker == null) {
        return null;
    }
    return parseInt(dayPicker.value);
}

function get_survey_settings() {
    var trigger_on_first_download = document.getElementById('trigger_on_first_download').checked;
    if (trackingSurvey) {
        var make_survey_always_available = document.getElementById('always_available').checked;
        return {
            'trigger_on_first_download': trigger_on_first_download,
            'randomize': scope.surveyBuilder.randomize,
            'randomize_with_memory': scope.surveyBuilder.randomizeWithMemory,
            'number_of_random_questions': scope.surveyBuilder.numberOfRandomQuestions,
            'always_available' : make_survey_always_available
        };
    } else if (audioSurvey) {
        var make_survey_always_available = document.getElementById('always_available').checked;
        var audioSurveyType = $("[name='audio_survey_type']:checked").val();
        ret = {'trigger_on_first_download': trigger_on_first_download,
                'audio_survey_type': audioSurveyType,
                'always_available' : make_survey_always_available};
        if (audioSurveyType == 'raw') { ret['sample_rate'] = parseInt($('#raw_options').val()); }
        if (audioSurveyType == 'compressed') { ret['bit_rate'] = parseInt($('#compressed_options').val()); }
        return ret;
    } else {
        var make_survey_always_available = document.getElementById('always_available').checked;
        ret = {'trigger_on_first_download': trigger_on_first_download,
                'always_available': make_survey_always_available };
        return ret;
    }
}

function end(domainName) {
    var content = '';
    if (trackingSurvey) {
        content = scope.surveyBuilder.questions;
        scope.surveyBuilder.errors = null;  // Reset errors
    } else if (audioSurvey) {
        content_list = [];
        // Remove double-quotes, which break the JSON parser.
        var voice_recording_prompt_text_input = $('#voice_recording_prompt_text_input').val();
        content_list.push( { prompt: voice_recording_prompt_text_input } );
        content = content_list;
    }
    $('.save_and_deploy_button').prop('disabled', true);  // Disable the buttons
    // Send a POST request (using XMLHttpRequest) with the JSON survey object as a parameter
    $.ajax({
        type: 'POST',
        url: '/update_survey/' + survey_id,
        data: {
            content: angular.toJson(content),
            timings: JSON.stringify(survey_times),
            settings: JSON.stringify(get_survey_settings())
        },
        statusCode: {
            200: function(response) {
                // A redirect to the Login page will show up as a 200 redirect
                alert("Your session may have expired. Open a new browser tab, log in to " + domainName + ", and then click the 'Save and Deploy' button again on this tab.");
            },
            201: function(response) {
                alert("Survey saved and submitted successfully!");
                location.reload();
            }
        }
    }).done(function() {
        // Don't do anything; this actually gets called BEFORE the statusCode functions
        $('.save_and_deploy_button').prop('disabled', false);  // Re-enable the buttons
    }).fail(function(response) {
        try {
            var errors = JSON.parse(response.responseText);
        } catch(e) {
            var errors = "";
        }
        if (_.has(errors, "duplicate_uuids")) {
            scope.surveyBuilder.errors = errors;
            scope.$apply();
        } else {
            alert("There was a problem with updating the survey, sorry!");
        }
        $('.save_and_deploy_button').prop('disabled', false);  // Re-enable the buttons
    });
}

// Turn the survey into a JSON object with an array of questions and other attributes
function createJsonSurveyObject() {
    var survey_id_string;
    survey_id_string = "WeeklySurveyCreatedAt" + new Date().getTime();
    var surveyObject = {
        hour_of_day: getHour(),
        questions: angular.element($("body")).scope().surveyBuilder.questions,
        survey_id: survey_id_string
    };
    // If it's a weekly survey, add the day of the week to ask the survey
    if (getDayOfWeek() != null) {
        surveyObject.day_of_week = getDayOfWeek();
    }
    console.log("surveyObject = ");
    console.log(surveyObject);
    return surveyObject;
}

//TODO CDUCMENCDHIsdcoihh
function audioSurveyTypeChange(audio_survey_type) {
    if (audio_survey_type == 'raw') {
        $("#compressed_options").hide();
        $("#raw_options").show();
    }
    else {
        $("#raw_options").hide();
        $("#compressed_options").show();
    }
}
