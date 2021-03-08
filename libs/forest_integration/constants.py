TREES = \
[
    'jasmine',
    'poplar',
    'willow',
]

TREE_METADATA = {
    'gps' : "gps data",
    'Acceleration' : "acceleraton data",
    'Poplar' : "poplar data",
}

FOREST_VERSION = "0.1"


# the following dictionary maps pairs of tree names and CSV fields to summary statistic names

# the first value of the output tuple is the summary statistic field name to be updated based on
# that value
# the second value is none, or a function with two inputs used to interpret that field into the
# summary statistic field. The function should take two parameters: the input field value, and the
# full line of data it appeared on (which should contain that value, among others)

# an example minutes to second conversion --- lambda value, _: value * 60
# an example using multiple fields:       --- lambda _, line: line['a'] * line['b']
#   where a and b are other csv fields

TREE_COLUMN_NAMES_TO_SUMMARY_STATISTICS = {
    ('gps', 'missing_time'): ('gps_data_missing_duration', None),
    ('gps', 'home_time'): ('home_duration', None),
    ('gps', 'max_dist_home'): ('distance_from_home', None),
    ('gps', 'dist_traveled'): ('distance_travelled', None),
    ('gps', 'av_flight_length'): ('flight_distance_average', None),
    ('gps', 'sd_flight_length'): ('flight_distance_standard_deviation', None),
    ('gps', 'av_flight_duration'): ('flight_duration_average', None),
    ('gps', 'sd_flight_duration'): ('flight_duration_standard_deviation', None),
    ('gps', 'diameter'): ('distance_diameter', None),
    # ('gps', ''): ('', None),
    # ('gps', ''): ('', None),
    # ('gps', ''): ('', None),
    # ('gps', ''): ('', None),
    # ('gps', ''): ('', None),
    # ('gps', ''): ('', None),

}