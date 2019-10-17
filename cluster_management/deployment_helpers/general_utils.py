# Do not import from other utils files here
import logging
import os
import random
import shutil
import string
import zipfile
from datetime import datetime
from os.path import join as path_join, split as path_split
from time import sleep

import botocore.exceptions as botoexceptions
import coloredlogs
from fabric.exceptions import NetworkError

coloredlogs.install(fmt="%(levelname)s %(name)s: %(message)s")

# Set logging levels
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('paramiko.transport').setLevel(logging.WARNING)

log = logging.getLogger("cluster-management")
log.setLevel(logging.DEBUG)


def current_time_string():
    """ used for pretty-printing the date in some logging statements"""
    return ("%s" % datetime.now())[:-7]


def EXIT(n=None):
    """ Ipython has some weirdness with exit statements. """
    if n is None:
        n = 0
    try:
        exit(n)
    except NameError:
        log.warn("ipython forcible exit...")
        sleep(0.05)
        import os
        os._exit(1)


def retry(func, *args, **kwargs):
    for i in range(100):
        try:
            return func(*args, **kwargs)
        except (NetworkError, botoexceptions.ClientError, botoexceptions.WaiterError) as e:
            log.error('Encountered error of type %s with error message "%s"\nRetrying with attempt %s.'
                      % (type(e).__name__, e, i+1) )
            sleep(3)


ALPHANUMERICS = string.ascii_letters + string.digits

def random_alphanumeric_string(length):
    return ''.join(random.choice(ALPHANUMERICS) for _ in range(length))

def random_alphanumeric_starting_with_letter(length):
    return random.choice(string.ascii_letters) + random_alphanumeric_string(length - 1)


def increment_identifier(base_string, increment_string):
    """ Finds the next number to use for an incrementing numerical suffix"""
    splits = increment_string.split(base_string)
    
    if len(splits) != 2:
        raise Exception("%s not found or found more than once in %s" % (base_string, increment_string))

    prefix, suffix = splits
    if suffix is "":
        suffix_increment = 1
    else:
        suffix_increment = int(suffix) + 1
    return prefix + str(suffix_increment)


# zip correction utilities
# the zip file that we download from github has a single folder in it, and elastic beanstalk wants it flat
def do_zip_reduction(file_name, absolute_folder_with_file_name_in_it, output_file_name):
    """ Removes the single root folder from the zip file, shifts everything up one level. """
    prior_directory = os.path.abspath(".")
    try:
        # move to the folder containing the file (simplifies other logic)
        os.chdir(absolute_folder_with_file_name_in_it)
        
        # Extract zip
        with zipfile.ZipFile(file_name) as f:
            file_paths = [some_file.filename for some_file in f.filelist]
            # determine the root folder...
            extraction_folder = path_split(os.path.commonprefix(file_paths))[0]
            if not extraction_folder:
                log.error("This zip file is already prepared for deployment, there was no common root folder in the zip file.")
                EXIT(1)
            else:
                log.info("Rebuilding zip file without root directiory.")
                # extract
                f.extractall()

        # get and read in the entire contents of the zip into a new zip, but one directory level up.
        with zipfile.ZipFile(output_file_name, mode='w', compression=zipfile.ZIP_DEFLATED) as z:
            for zip_path, file_path in get_file_paths_for_zipping(extraction_folder):
                with open(file_path) as f:
                    z.writestr(zip_path, f.read())
        
        # delete the extraction folder
        shutil.rmtree(extraction_folder)
        
    except None:
        pass
    finally:
        # we always want to return to the original directory.
        os.chdir(prior_directory)
        

def get_file_paths_for_zipping(folder_path):
    """ Provides a list of tuples of zip_file_path, real_file_path """
    file_paths = []
    # the _ is a list of directories discovered in the walk
    for root, _, filenames in os.walk(folder_path):
        try:
            operational_root = root.split("/", 1)[1]
        except IndexError:
            operational_root = ""
        for filename in filenames:
            zip_path = path_join(operational_root, filename)
            file_path = path_join(root, filename)
            file_paths.append((zip_path, file_path))
    return file_paths
