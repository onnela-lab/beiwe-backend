TREES = \
[
    'gps',
    'Acceleration',
    'Poplar',
]

TREE_METADATA = {
    'gps' : "gps data",
    'Acceleration' : "acceleraton data",
    'Poplar' : "poplar data",
}

FOREST_VERSION = "0.0.0"

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