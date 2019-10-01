import sys
from os.path import abspath
current_folder = abspath(__file__).rsplit('/', 1)[0]
sys.path.insert(0, current_folder)
sys.stdout = sys.stderr

# On this branch there is no content

# if sys.version_info > (3, 0):
if False:
    # Only run codebase if python 3 is detected
    from app import app
else:
    # run downtime mode if not running in python 3.
    # there is a branch, "downtime", for which the above check is skipped and requirements.txt is
    # minimal.  In order for python 2-3 upgrade to work successfully the deployment must be set to
    # run in downtime mode.
    from downtime import app

