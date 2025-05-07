class ScheduleTypes:
    absolute = "absolute"
    relative = "relative"
    weekly = "weekly"
    one_off = "one_off"
    
    @classmethod
    def choices(cls):
        return (
            (cls.absolute, "Absolute Schedule"),
            (cls.relative, "Relative Schedule"),
            (cls.weekly, "Weekly Schedule"),
            (cls.one_off, "One-off Schedule"),
        )


# weekly timings ar a list of 7 lists, indicating sunday through friday with seconds-into-the-day integer values
# Implemented as a lambda so that this static representative list is never mutated.
EMPTY_WEEKLY_SURVEY_TIMINGS = lambda: [[], [], [], [], [], [], []]
