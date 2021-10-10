WEEKLY_SCHEDULE_KEY = "timings"  # predates the other schedules
ABSOLUTE_SCHEDULE_KEY = "absolute_timings"
RELATIVE_SCHEDULE_KEY = "relative_timings"
INTERVENTIONS_KEY = "interventions"

# keys used if various places, pulled out into constants for consistency.
DEVICE_SETTINGS_KEY = 'device_settings'
STUDY_KEY = 'study'
SURVEY_CONTENT_KEY = 'content'
# SURVEY_SETTINGS_KEY = 'settings'
SURVEYS_KEY = 'surveys'

# "_id" is legacy, pk shouldn't occur naturally.  This list applies to Surveys and Studies.
NEVER_EXPORT_THESE = ('_id', 'id', 'pk', 'created_on', 'last_updated', 'object_id', "deleted")

# new stuff
INTERVENTION__NAME = "intervention__name"
survey_params = ("content", "survey_type", "settings")
weekly_params = ("day_of_week", "hour", "minute")
absolute_params = ("date", "hour", "minute")
relative_params = (INTERVENTION__NAME, "days_after", "hour", "minute")

BAD_DEVICE_SETTINGS_FIELDS = ("created_on", "last_updated", "study")
