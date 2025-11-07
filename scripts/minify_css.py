import sys
from os import walk
from os.path import abspath, join as path_join, relpath

from rcssmin import cssmin


# hack that inserts the root of the project folder into the python path so we can import the codebase
repo_root = abspath(__file__).rsplit('/', 2)[0]
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from constants.common_constants import BEIWE_PROJECT_ROOT


css_folder = path_join(BEIWE_PROJECT_ROOT, "frontend/static/css")

assert repo_root == BEIWE_PROJECT_ROOT

"""
at time of writing these are the files created:
	frontend/static/css/admin.min.css
	frontend/static/css/dashboard_pages.min.css
	frontend/static/css/datepicker.min.css
	frontend/static/css/empty_css_for_tests.min.css
	frontend/static/css/libraries/bootstrap-darkly.min.css
	frontend/static/css/libraries/bootstrap-datetimepicker.min.css
	frontend/static/css/libraries/bootstrap-timepicker.min.css
	frontend/static/css/libraries/datatables.min.css
	frontend/static/css/survey_builder.min.css
"""

# walk the css folder and minify all .css files.  this will overwrite existing minified files
for root, _dirs, paths in walk(css_folder):
    
    if  "frontend/static/css/phone" in root:  # do not process these files
        print("skipping folder: ", relpath(root))
        continue
    
    for path in paths:
        
        if not path.endswith(".css"):  # just css
            continue
        
        full_path = path_join(root, path)
        
        if path.endswith(".min.css"):  # skip already minified files
            print("Skipping already minified file:", full_path)
            continue
        
        out_path = full_path.replace(".css", ".min.css")
        print("Minifying:", full_path, " -> ", out_path)
        
        # read, minify, write
        with open(full_path, "r", encoding="utf-8") as f:
            css_content = f.read()
        minified_css: str = cssmin(css_content)  # type: ignore
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(minified_css)
