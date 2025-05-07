from django.urls import URLPattern


# used to indicate if a url redirect is safe to redirect to
IGNORE = "IGNORE"
SAFE = "SAFE"

# These are declared here so that they can be imported, they are populated in urls.py.
LOGIN_REDIRECT_IGNORE: list[URLPattern] = []
LOGIN_REDIRECT_SAFE: list[URLPattern] = []
# urlpatterns probably needs to be lowercase for django to find it
urlpatterns: list[URLPattern] = []
