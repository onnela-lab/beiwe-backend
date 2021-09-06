$(document).ready(function(){
    $('#many-new-patients-loading-spinner').hide();

    $('#createManyPatientsForm').on('submit', function(e){
        $('#createManyPatientsButton').prop('disabled', true);  // Disable the button so they can't click it twice
        $('#many-new-patients-loading-spinner').show();
        $('#many-new-patients-submit-and-cancel-buttons').hide();
    });

    $('#addManyPatientsModal').on("hidden.bs.modal", function() {
        location.reload();
    });
});

function logout() {
    window.location.href="/logout";
}

/* Pop up a confirmation dialog and only delete the study if the user types an EXACT string */
function confirm_delete_study(study_name, study_id) {
    var required_matching_text = "Yes, I want to delete " + study_name;
    var prompt_message = "Are you ABSOLUTELY SURE that you want to delete the study " + study_name +" and all users and surveys associated with it?\nIf you're DEAD CERTAIN, type '" + required_matching_text + "' in the box here:";
    var confirmation_prompt = prompt(prompt_message);
    if (confirmation_prompt == required_matching_text) {
        $.ajax({
            type: 'POST',
            url: '/delete_study/' + study_id,
            data: {
                confirmation: "true"
            }
        }).done(function() {
            location.href = "/manage_studies";
        }).fail(function() {
            alert("There was a problem with deleting the study.");
        });
    };
}

/* Pop up a confirmation dialog and only delete the custom field if the user types an EXACT string */
function confirm_delete_custom_field(field_name, field_id, study_id) {
    var required_matching_text = "Yes, I want to delete " + field_name;
    var prompt_message = ("Are you ABSOLUTELY SURE that you want to delete the custom field " +
                          field_name +" from this study?\nThis will also delete all data " +
                          "in that custom field on this study.\nIf you're DEAD CERTAIN, type '" +
                          required_matching_text + "' in the box here:");
    var confirmation_prompt = prompt(prompt_message);
    if (confirmation_prompt == required_matching_text) {
        $.ajax({
            type: 'POST',
            url: '/delete_field/' + study_id,
            data: {
                field: field_id,
                confirmation: "true"
            }
        }).done(function() {
            location.href = "/study_fields/" + study_id;
        }).fail(function() {
            alert("There was a problem with deleting the custom_field.");
        });
    };
}

function confirm_delete_intervention(intervention_name, intervention_id, study_id) {
    var required_matching_text = "Yes, I want to delete " + intervention_name;
    var prompt_message = ("Are you ABSOLUTELY SURE that you want to delete the custom field " +
                          intervention_name +" from this study?\nThis will also delete all data " +
                          "in that custom field on this study.\nIf you're DEAD CERTAIN, type '" +
                          required_matching_text + "' in the box here:");
    var confirmation_prompt = prompt(prompt_message);
    if (confirmation_prompt == required_matching_text) {
        $.ajax({
            type: 'POST',
            url: '/delete_intervention/' + study_id,
            data: {
                intervention: intervention_id,
                confirmation: "true"
            }
        }).done(function() {
            location.href = "/interventions/" + study_id;
        }).fail(function() {
            alert("There was a problem with deleting the intervention")
        });
    };
}
