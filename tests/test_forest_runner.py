import csv
from datetime import date
from io import StringIO
from typing import Dict, List

from constants.forest_constants import ForestTree
from database.forest_models import ForestTask, SummaryStatisticDaily
from services.celery_forest import BadForestField, csv_parse_and_consume
from tests.common import CommonTestCase


class NopeBadCSV(Exception): pass


class TestFileConsumption(CommonTestCase):
    
    def call_csv_parse_and_consume(self, task: ForestTask, csv_dict_rows: List[Dict]):
        """ We need to test with a real dictreader, this function converts a list of dictionaries to
        an in-memory csv file and then calls csv_parse_and_consume on it. """
        if len(csv_dict_rows) < 1:
            raise NopeBadCSV("csv_dict_rows must have at least one row, I'm not a magician")
        
        column_names = list(csv_dict_rows[0].keys())  # order matters
        column_names_set = set(column_names)
        
        # check that the dictionaries all have the same row keys, that's otherwise its not a csv
        for row in csv_dict_rows:
            self.assertSetEqual(column_names_set, set(row.keys()))
        
        # Prepend the keys, then get the values for each row (in that order), then join each item
        # with a comma, then join all rows with a newline.
        csv_string = ",".join(column_names) + "\n" + \
            "\n".join(
                ",".join([str(row[key]) for key in column_names]) for row in csv_dict_rows
            )
        
        # shove the csv string into a file-like object and call.
        return csv_parse_and_consume(task, csv.DictReader(StringIO(csv_string)))
    
    def one_jasmine_row(self, day: date):
        # keep this list ordered to match the order of the matching columns in the
        # SummaryStatistcsDaily table, which is in turn supposed to be ordered to match the order
        # as declared in TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS, plus the date columns
        return {
            'year': day.year,            # ! year - int
            'month': day.month,          # ! month of year - int
            'day': day.day,              # ! day of month - int
            "diameter": 0.0,             # FloatField
            "max_dist_home": 0.0,        # FloatField
            "dist_traveled": 0.0,        # FloatField
            "av_flight_length": 0.0,     # FloatField
            "sd_flight_length": 0.0,     # FloatField
            "av_flight_duration": 0.0,   # FloatField
            "sd_flight_duration": 0.0,   # FloatField
            "missing_time": 0,           # IntegerField
            "home_time": 0.0,            # FloatField
            "radius": 0.0,               # FloatField
            "num_sig_places": 0,         # IntegerField
            "entropy": 0.0,              # FloatField
            "total_pause_time": "0",     # TextField
            "obs_duration": 0.0,         # FloatField
            "obs_day": 0.0,              # FloatField
            "obs_night": 0.0,            # FloatField
            "total_flight_time": 0.0,    # FloatField
            "av_pause_duration": 0.0,    # FloatField
            "sd_pause_duration": 0.0,    # FloatField
        }
    
    def test_csv_parse_and_consume_jasmine(self):
        self.default_forest_task.update(
            data_date_start=date(2020, 1, 3),
            data_date_end=date(2020, 1, 10),
            forest_tree=ForestTree.jasmine,
        )
        # test two days inside the range
        csv_dict_rows = [
            self.one_jasmine_row(date(2020, 1, 5)),
            self.one_jasmine_row(date(2020, 1, 6)),
        ]
        
        self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
        self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
        self.assertEqual(SummaryStatisticDaily.objects.count(), 2)
    
    def one_willow_row(self, day: date):
        return {
            'year': day.year,                        # ! year - int
            'month': day.month,                      # ! month of year - int
            'day': day.day,                          # ! day of month - int
            "num_r": 0,                              # IntegerField
            "num_r_tel": 0,                          # IntegerField
            "total_char_r": 0,                       # IntegerField
            "num_s": 0,                              # IntegerField
            "num_s_tel": 0,                          # IntegerField
            "total_char_s": 0,                       # IntegerField
            "text_reciprocity_incoming": 0,          # IntegerField
            "text_reciprocity_outgoing": 0,          # IntegerField
            "num_mms_s": 0,                          # IntegerField
            "num_mms_r": 0,                          # IntegerField
            "num_in_call": 0,                        # IntegerField
            "num_in_caller": 0,                      # IntegerField
            "total_mins_in_call": 0.0,               # FloatField
            "num_out_call": 0,                       # IntegerField
            "num_out_caller": 0,                     # IntegerField
            "total_mins_out_call": 0.0,              # FloatField
            "num_mis_call": 0,                       # IntegerField
            "num_mis_caller": 0,                     # IntegerField
            "num_uniq_individuals_call_or_text": 0,  # IntegerField
        }
    
    def test_csv_parse_and_consume_willow(self):
        self.default_forest_task.update(
            data_date_start=date(2020, 1, 3),
            data_date_end=date(2020, 1, 10),
            forest_tree=ForestTree.willow,
        )
        csv_dict_rows = [
            self.one_willow_row(date(2020, 1, 5)),
            self.one_willow_row(date(2020, 1, 6)),
        ]
        
        self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
        self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
        self.assertEqual(SummaryStatisticDaily.objects.count(), 2)
    
    def one_sycamore_row(self, day: date):
        return {
            'year': day.year,                        # ! year - int
            'month': day.month,                      # ! month of year - int
            'day': day.day,                          # ! day of month - int
            "num_surveys": 0,                        # IntegerField
            "num_complete_surveys": 0,               # IntegerField
            "num_opened_surveys": 0,                 # IntegerField
            "avg_time_to_submit": 0.0,               # FloatField
            "avg_time_to_open": 0.0,                 # FloatField
            "avg_duration": 0.0,                     # FloatField
        }
    
    def test_csv_parse_and_consume_sycamore(self):
        self.default_forest_task.update(
            data_date_start=date(2020, 1, 3),
            data_date_end=date(2020, 1, 10),
            forest_tree=ForestTree.sycamore,
        )
        csv_dict_rows = [
            self.one_sycamore_row(date(2020, 1, 5)),
            self.one_sycamore_row(date(2020, 1, 6)),
        ]
        
        self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
        self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
        self.assertEqual(SummaryStatisticDaily.objects.count(), 2)
    
    def one_oak_row(self, day: date):
        return {
            'date': day.isoformat(),            # ! day of month - isoformat
            "walking_time": 0,                  # IntegerField
            "steps": 0,                         # IntegerField
            "cadence": 0,                       # IntegerField
        }
    
    def test_oak_parse_and_consume(self):
        self.default_forest_task.update(
            data_date_start=date(2020, 1, 3),
            data_date_end=date(2020, 1, 10),
            forest_tree=ForestTree.oak,
        )
        csv_dict_rows = [
            self.one_oak_row(date(2020, 1, 5)),
            self.one_oak_row(date(2020, 1, 6)),
        ]
        
        self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
        self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
        self.assertEqual(SummaryStatisticDaily.objects.count(), 2)
    
    #
    ## the rest of the tests will use jasmine as the default forest tree
    #
    
    def test_range_inclusive(self):
        self.default_forest_task.update(
            data_date_start=date(2020, 1, 1),
            data_date_end=date(2020, 1, 10),
            forest_tree=ForestTree.jasmine,
        )
        csv_dict_rows = [
            self.one_jasmine_row(date(2019, 12, 30)),
            self.one_jasmine_row(date(2019, 12, 31)),  # december has 31 days
            self.one_jasmine_row(date(2020, 1, 1)),
            self.one_jasmine_row(date(2020, 1, 2)),
            self.one_jasmine_row(date(2020, 1, 3)),
            self.one_jasmine_row(date(2020, 1, 4)),
            self.one_jasmine_row(date(2020, 1, 5)),
            self.one_jasmine_row(date(2020, 1, 6)),
            self.one_jasmine_row(date(2020, 1, 7)),
            self.one_jasmine_row(date(2020, 1, 8)),
            self.one_jasmine_row(date(2020, 1, 9)),
            self.one_jasmine_row(date(2020, 1, 10)),
            self.one_jasmine_row(date(2020, 1, 11)),
        ]
        
        self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
        self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
        self.assertEqual(SummaryStatisticDaily.objects.count(), 10)
        for d in SummaryStatisticDaily.objects.values_list("date", flat=True):
            self.assertGreaterEqual(d, date(2020, 1, 1))
            self.assertLessEqual(d, date(2020, 1, 10))
    
    def test_bad_column_name(self):
        self.default_forest_task.update(
            data_date_start=date(2020, 1, 1),
            data_date_end=date(2020, 1, 10),
            forest_tree=ForestTree.jasmine,
        )
        csv_dict_rows = [
            # this row has an extra column
            {**self.one_jasmine_row(date(2020, 1, 5)), "extra_column": 0.0},
        ]
        
        self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
        with self.assertRaises(BadForestField):
            self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
        self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
    
    # def test_bad_column_name_in_one_row(self):
    #     ## NOPE that's not how csv.DictReader works, all rows will find the extra column and raise
    #     self.default_forest_task.update(
    #         data_date_start=date(2020, 1, 1),
    #         data_date_end=date(2020, 1, 10),
    #         forest_tree=ForestTree.jasmine,
    #     )
    
    #     # now ensure that we still consume the rest of the rows if one has that clumns
    #     csv_dict_rows = [
    #         self.one_jasmine_row(date(2020, 1, 4)),
    #         {**self.one_jasmine_row(date(2020, 1, 5)), "extra_column": 0.0},
    #     ]
    #     with self.assertRaises(BadForestField):
    #         self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
    #     self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
    
    #     # ensure behavior is order-independent
    #     csv_dict_rows = [
    #         {**self.one_jasmine_row(date(2020, 1, 5)), "extra_column": 0.0},
    #         self.one_jasmine_row(date(2020, 1, 4)),
    #     ]
    #     with self.assertRaises(BadForestField):
    #         self.call_csv_parse_and_consume(self.default_forest_task, csv_dict_rows)
    #     self.assertEqual(SummaryStatisticDaily.objects.count(), 0)
