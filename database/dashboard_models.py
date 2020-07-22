from django.db import models

from database.models import TimestampedModel


class DashboardColorSetting(TimestampedModel):
    """ Database model, details of color settings point at this model. """
    data_type = models.CharField(max_length=32)
    study = models.ForeignKey("Study", on_delete=models.PROTECT, related_name="dashboard_colors")

    class Meta:
        # only one of these color settings per-study-per-data type
        unique_together = (("data_type", "study"),)

    def get_dashboard_color_settings(self):
        # return a (json serializable) dict of a dict of the gradient and a list of dicts for
        # the inflection points.

        # Safely/gracefully access the gradient's one-to-one field.
        try:
            gradient = {
                "color_range_min": self.gradient.color_range_min,
                "color_range_max": self.gradient.color_range_max,
            }
        except DashboardGradient.DoesNotExist:
            gradient = {}

        return {
            "gradient": gradient,
            "inflections": list(self.inflections.values("operator", "inflection_point")),
        }

    def gradient_exists(self):
        try:
            if self.gradient:
                return True
        except DashboardGradient.DoesNotExist:
            # this means that the dashboard gradient does not exist in the database
            return False


class DashboardGradient(TimestampedModel):
    # It should be the case that there is only one gradient per DashboardColorSettings
    dashboard_color_setting = models.OneToOneField(
        DashboardColorSetting, on_delete=models.PROTECT, related_name="gradient", unique=True,
    )

    # By setting both of these to 0 the frontend will automatically use tha biggest and smallest
    # values on the current page.
    color_range_min = models.IntegerField(default=0)
    color_range_max = models.IntegerField(default=0)


class DashboardInflection(TimestampedModel):
    # an inflection corresponds to a flag value that has an operator to display a "flag" on the dashboard front end
    dashboard_color_setting = models.ForeignKey(
        DashboardColorSetting, on_delete=models.PROTECT, related_name="inflections"
    )

    # these are a mathematical operator and a numerical "inflection point"
    # no default for the operator, default of 0 is safe.
    operator = models.CharField(max_length=1)
    inflection_point = models.IntegerField(default=0)
