$(document).ready(function(){
    // Set up the main list of participants using DataTables
    $("#participantList").DataTable({
        "processing": true,
        "serverSide": true,
        "ajax": {
            url: "/study/" + studyId + "/get_participants_api/",
            type: 'POST'  // Get request size ballooned past 4k characters, so use POST
        },
        "order": [1, 'asc'],
        "columnDefs": [
            // Format the second column (index 1) to be a link to the View Participant page
            {"targets": 2, "render": function(data, type, row, meta) {
                if(type === 'display') {
                    data = '<b><a class="link-fill" href="/view_study/' + studyId + '/participant/' + data + '">' + data + '</a></b>'
                }
                return data;
            }},
            // You can sort the table on any of the first 4 columns
            // {"targets": [0, 1, 2, 3], "orderable": true},
            // You can now sort the table on any other columns (custom fields & intervention dates)
            {"targets": "_all", "orderable": true},
        ]
    });
});
