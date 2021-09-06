$(document).ready(function(){
    // Set up the main list of participants using DataTables
    $("#participantList").DataTable({
        "processing": true,
        "serverSide": true,
        "ajax": "/study/" + studyId + "/get_participants_api",
        "columnDefs": [
            // Format the second column (index 1) to be a link to the View Participant page
            {"targets": 1, "render": function(data, type, row, meta) {
                if(type === 'display') {
                    data = '<b><a href="/view_study/' + studyId + '/participant/' + data + '">' + data + '</a></b>'
                }
                return data;
            }},
            // You can sort the table on any of the first 4 columns
            {"targets": [0, 1, 2, 3], "orderable": true},
            // You can't sort the table on any other columns (custom fields & intervention dates)
            {"targets": "_all", "orderable": false},
        ]
    });
});
