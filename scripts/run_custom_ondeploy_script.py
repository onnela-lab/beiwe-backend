from sys import argv

from constants.common_constants import CUSTOM_ONDEPLOY_SCRIPT_EB, CUSTOM_ONDEPLOY_SCRIPT_PROCESSING
from libs.s3 import s3_list_files, s3_retrieve_plaintext
from libs.sentry import SentryUtils


def main():
    if "elasticbeanstalk" in argv:
        script_folder = CUSTOM_ONDEPLOY_SCRIPT_EB
        error_sentry = SentryUtils.report_webserver()
    elif "processing" in argv:
        script_folder = CUSTOM_ONDEPLOY_SCRIPT_PROCESSING
        error_sentry = SentryUtils.report_data_processing()
    else:
        raise Exception(f"Must supply either 'elasticbeanstalk' or 'processing' as an argument, found `{argv}`")
    
    sorted_files: list[str] = list(s3_list_files(script_folder))
    sorted_files.sort()
    
    for file_name in sorted_files:
        if file_name.endswith(".py"):
            # with error_sentry():
            python_file_contents = s3_retrieve_plaintext(file_name).decode()
            print()
            print(f"Running {file_name}")
            print()
            print(python_file_contents)
            print()
            exec(python_file_contents, globals())
            print()
            print(f"Done running {file_name}")
            print()
        
        if error_sentry.errors:
            error_sentry.raise_errors()  # stop execution if there are errors
