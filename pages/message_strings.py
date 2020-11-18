ALERT_ANDROID_DELETED_TEXT = \
    """
    <h3>Stored Android Firebase credentials have been removed if they existed!</h3>
    <p>All registered android apps will be updated as they connect. That process may take some time</p>
    """

ALERT_ANDROID_SUCCESS_TEXT = \
    """
    <h3>New android credentials were received!</h3>
    <p>All registered android apps will be updated as they connect. That process may take some time</p>
    """

ALERT_ANDROID_VALIDATION_FAILED_TEXT = \
    """
    <div class="alert alert-danger" role="alert">
        <h3>There was an error in the processing the new android credentials!</h3>
        <p>We are expecting a json file with a "project_info" field containing at least a "storage_bucket", 
        "firebase_url", and "project_id"</p>
    </div>
    """

ALERT_DECODE_ERROR_TEXT = \
    """
    <div class="alert alert-danger" role="alert">
        <h3>There was an error in the reading the new credentials!</h3>
        <p>We were unable to read the uploaded file. Make sure that the credentials are saved in a plaintext format 
        with an extension like ".txt" or ".json", and not a format like ".pdf" or ".docx"</p>
    </div>
    """

ALERT_EMPTY_TEXT = \
    """
    <div class="alert alert-danger" role="alert">
        <h3>There was an error in the processing the new credentials!</h3>
        <p>You have selected no file or an empty file. If you just want to remove credentials, use the 
        delete button</p>
        <p>The previous credentials, if they existed, have not been removed</p>
    </div>
    """

ALERT_FIREBASE_DELETED_TEXT = \
    """
    <h3>All backend Firebase credentials have been deleted if they existed!</h3>
        <p>Note that this does not include IOS and Android app credentials, these must be deleted separately if 
        desired</p>
    """

ALERT_IOS_DELETED_TEXT = \
    """
    <h3>Stored IOS Firebase credentials have been removed if they existed!</h3>
    <p>All registered IOS apps will be updated as they connect. That process may take some time</p>
    """
ALERT_IOS_SUCCESS_TEXT = \
    """
    <h3>New IOS credentials were received!</h3>
    <p>All registered IOS apps will be updated as they connect. That process may take some time</p>
    """

ALERT_IOS_VALIDATION_FAILED_TEXT = \
    """
    <div class="alert alert-danger" role="alert">
        <h3>There was an error in the processing the new IOS credentials!</h3>
        <p>We are expecting a plist file with at least the following keys: "STORAGE_BUCKET", "API_KEY" and 
        "DATABASE_URL"</p>
    </div>
    """

ALERT_MISC_ERROR_TEXT = \
    """
    <div class="alert alert-danger" role="alert">
        <h3>There was an error in the processing the new credentials!</h3>
    </div>
    """

ALERT_SUCCESS_TEXT = \
    """<h3>New firebase credentials have been received!</h3>"""

