import csv
from datetime import date
from io import StringIO

from database.tableau_api_models import SummaryStatisticDaily
from database.user_models import Participant
from libs.forest_integration.constants import TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS


def construct_summary_statistics(study, participant, tree_name, csv_string):
    if not participant and participant.study:
        raise ValueError("no participant or study associated with Forest data")
    file = StringIO(csv_string)  # this imitates the file interface to allow reading as a CSV
    with open(file, 'rb') as f:
        reader = csv.DictReader(f)
        data = list(reader)

    #TODO: ensure data is only in time range assossiated with forest tracker
    for line in data:
        summary_date = date(year=line['year'], month=line['month'], day=line['day'])
        updates = {}
        for column_name, value in line.items():
            if (tree_name, column_name) in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS:
                summary_stat_field, interp_function = TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS[(tree_name, column_name)]
                if interp_function is not None:
                    updates[summary_stat_field] = interp_function(value, line)
                else:
                    updates[summary_stat_field] = value

        if len(updates) != len([k for k in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS.keys() if k[0] == tree_name]):
            # error instead? to error log?
            print('some fields not found in forest data output, possible missing data. '
                  'Check if you are using an outdated version of Forest')

        obj, created = SummaryStatisticDaily.objects.update_or_create(
            study=study,
            participant=participant,
            date=summary_date,
            defaults=updates
        )

