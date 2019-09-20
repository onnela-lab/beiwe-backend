import json, urllib, urllib2, requests

# Comment out the following import to disable the credentials file.
try:
    from my_data_access_api_credentials import ACCESS_KEY, SECRET_KEY
except ImportError:
    ACCESS_KEY, SECRET_KEY = None, None

API_URL_BASE = 'https://staging.beiwe.org/'

from pprint import pprint
def make_pipeline_request(study_id, access_key=ACCESS_KEY, secret_key=SECRET_KEY, tags=None):
    """
    Behavior This function will download the data from the server, decompress it, and WRITE IT TO
    FILES IN YOUR CURRENT WORKING DIRECTORY. If the data in the current working directory
    includes a "registry.dat" file, the server will use the contents of it to only download files
    that are new, potentially greatly speeding up your requests.

    Study ID study_id is required for any query. Currently the only way to view the ID of a given
    study on studies.beiwe.org website is to look at the url while viewing a study's main page.
    study_id is a string, it will look like this: 55f9d1a597013e3f50ffb4c7

    Credentials If you are using the my_data_access_credentials.py file your credentials will
    automatically be pulled from it; you will not need to provide those as kwargs. Simply fill in
    the appropriate access and secret key values to the variables in that file. You can find your
    credentials on the studies.beiwe.org website, click the Manage Credentials tab on the top of
    the page.
    
    Tags Provide a list to tags.  Any pipeline files that matche any tags will be downloaded.  If
    you provide an empty list of tags no data will be returned.  If you provide no tags at all (a
    None, the default value) you will get all files that match the study.
    """
    
    if access_key is None or secret_key is None:
        raise Exception("You must provide credentials to run this API call.")
    
    url = API_URL_BASE + 'get-pipeline/v1'
    values = {
        'access_key': access_key,
        'secret_key': secret_key,
        'study_id': study_id,
    }
    if tags is not None:
        if not isinstance(tags, list):
            raise TypeError("tags must be a list, received %s" % type(tags))
        values['tags'] = json.dumps(tags)
    
    print("sending request, this could take some time.")
    # print values
    try:
        req = urllib2.Request(url, urllib.urlencode(values))
        response = urllib2.urlopen(req)
    except Exception as e:
        print("\n\nERROR\n\n")
        pprint(vars(e))
        print("\n\n")
        return e

    return_data = response.read()
    
    print("writing file to the_downloaded_data.zip")
    with open("the_downloaded_data.zip", "w") as f:
        f.write(return_data)
    
    print("Operations complete.")


def upload_pipeline_file(study_id, file_path, name_of_file_in_future_downloaded_zip,
                         access_key=ACCESS_KEY, secret_key=SECRET_KEY, tags=None):
    
    if tags is None:
        tags = []
        
    with open(file_path, "rb") as f:
        r = requests.post(
                API_URL_BASE + "pipeline-upload/v1",
                files={'file': f},
                data={
                    "study_id": study_id,
                    "access_key": access_key,
                    "secret_key": secret_key,
                    "tags": json.dumps(tags),
                    "file_name":name_of_file_in_future_downloaded_zip,
                }
        )
    
    if r.status_code != 200:
        print(r.status_code)
        print(r._content)
        
    return r
    