$(document).ready(function() {
	/* Set up Date/Time pickers */
	settings = {
		format: 'YYYY-MM-DD  HH:00',
		// stepping: 60,
		sideBySide: true,
		toolbarPlacement: "bottom",
		showClear: true,
		showClose: true,
		icons:{
			clear: 'glyphicon glyphicon-trash text-danger',
			close: 'glyphicon glyphicon-check text-success',
		},
		minDate: moment("2016-08-01"),  // 2016-8-1 there can be no data before this.
	}
	$("#start_datetimepicker").datetimepicker(settings)
	$('#end_datetimepicker').datetimepicker(settings)
	
	/* When the form gets submitted, reformat the DateTimes into ISO UTC format */
	$('#data_download_parameters_form').submit(submit_clicked)
})

function submit_clicked() {
	format_datetimepickers()
	toggle_ready_submit_button()
	return true
}

function toggle_compress() {
	$('#compress_info').toggle()
	form = $('#data_download_parameters_form')
	
	if (form.attr("action") == "/get-data/v1") {  // change the target url to download compressed data.
		form.attr("action", "/get-data/v2")
	} else {
		form.attr("action", "/get-data/v1")
	}
}

function toggle_ready_submit_button() {
	$('#download_ready_button').toggle()
	$('#submit_button_div').toggle()
	$("#explanation_paragraph").toggle()
	$('#compress_div').toggle()
}

function format_datetimepickers() {
	extract_date_for_submit($('#start_datetime'), $('#secret1'))
	extract_date_for_submit($('#end_datetime'), $('#secret2'))
}

// The datetime fields can't be present if they contain "", so we load/hide the value from a hidden
// input field. We do the same thing if the user enters a bad value.
function extract_date_for_submit(picker, secret_input) {
	var prior_str = picker.val()
	
	if (!prior_str || prior_str == "Invalid date") {
		secret_input.val("")
		secret_input.attr('disabled', 'disabled')  // it doesn't want even an empty string
	} else {
		var prior_formatted = moment(prior_str).format('YYYY-MM-DDTHH:00:00')  // parse
		secret_input.val(prior_formatted)
		secret_input.removeAttr('disabled')
	}
}
