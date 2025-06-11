$(document).ready(function() {
	/* Set up Date/Time pickers */
	$('#start_datetimepicker').datetimepicker();
	$('#end_datetimepicker').datetimepicker();
	
	/* When the form gets submitted, reformat the DateTimes into ISO UTC format */
	$('#data_download_parameters_form').submit(submit_clicked);
});


function submit_clicked() {
	format_datetimepickers();
	toggle_ready_submit_button()
	return true;
};

function toggle_compress() {
	$('#compress_info').toggle();
	form = $('#data_download_parameters_form');
	
	if (form.attr("action") == "/get-data/v1") {
		form.attr("action", "/get-data/v2")
	} else {
		form.attr("action", "/get-data/v1")
	}
}

function toggle_ready_submit_button() {
	$('#download_ready_button').toggle()
	$('#submit_button_div').toggle()
	$("#explanation_paragraph").toggle()
}

function enable_submit_button() {
	toggle_ready_submit_button()
	$('#start_datetime').attr('disabled', false);
	$('#end_datetime').attr('disabled', false);
	if (prior_end) {
		$('#end_datetime').val(prior_end);
	}
	if (prior_start) {
		$('#start_datetime').val(prior_start);
	}
}

// we need to toggle these between different states
prior_start = ""
prior_end = ""

function format_datetimepickers() {
	start = $('#start_datetime')
	end = $('#end_datetime')	
	// stash to restore
	prior_start = start.val()
	prior_end = end.val()
	// set format to match api
	start.val(moment(start.val()).format('YYYY-MM-DDTHH:mm:ss'));
	end.val(moment(end.val()).format('YYYY-MM-DDTHH:mm:ss'));
	start.attr('disabled', 'disabled');
	end.attr('disabled', 'disabled');
	//handle case of empty (and forced-bad) dates
	if (start.val() == "Invalid date") {
		start.val("");
		prior_start = "";
	}
	if (end.val() == "Invalid date") {
		end.val("");
		prior_end = "";
	}
};
