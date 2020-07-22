import smtplib
import traceback
from datetime import datetime

from cronutils.error_handler import BundledError

from config.constants import E500_EMAIL_ADDRESS, OTHER_EMAIL_ADDRESS
from config.settings import SYSADMIN_EMAILS


def log_and_email_500_error(e, log_message=None, emails=SYSADMIN_EMAILS):
    """ Prints in the server logs (defaults to Apache if not specified), does NOT stop execution.
    Note the error message value is actually the subject line of the email, the "log_message"
    variable is a passed in as the "message" variable into the log_error function and appears as
    part of the email and log statement. """
    try:
        subject = "Beiwe Error: %s" % e
        message = log_error(e, log_message, reraise=True)
        _send_email(E500_EMAIL_ADDRESS, emails, message, subject)
        
    except Exception:
        print("\n!!!! ERROR IN log_and_email_error !!!!")


def email_bundled_error(bundled_error, subject, emails=SYSADMIN_EMAILS):
    # use the __repr__() function to extract an emailable version of the bundled error.
    if not isinstance(bundled_error, BundledError):
        print("ERROR: received a non-BundledError in email_bundled_error.")
        raise bundled_error
    _send_email(OTHER_EMAIL_ADDRESS, emails, bundled_error.__repr__(), subject)


def log_error(e, message=None, reraise=False):
    """ Prints an error to the apache log. "message" is a customizable that will be printed in
    the log.
    Reraise is dangerous, only set to true if you understand what it is.
    natively handles BundledError error formatting. """
    try:
        error_message = "===================\n"
        error_message += datetime.utcnow().isoformat() + "\n"
        if message is not None: error_message += message + "\n"
        if isinstance(e, BundledError):
            error_message += e.__repr__()
        else:
            error_message += "ERROR:" + str(e.__repr__()) + "\n"
            error_message += traceback.format_exc() + "\n"
            error_message += "===================\n"
        print(error_message)
        return error_message
    except Exception:
        print("\n!!!! ERROR IN log_error !!!!")
        if reraise:
            raise


def email_system_administrators(message, subject, source_email=OTHER_EMAIL_ADDRESS):
    """ Sends an email to the system administrators. """
    try:
        _send_email(source_email, SYSADMIN_EMAILS, message, subject)
    except Exception as e:
        # todo: low priority. this reraise parameter may be incorrect.
        log_error(e, message="sysadmin email failed", reraise=False)


def _send_email(source_email_address, destination_email_addresses, message, subject):
    """ Inner function for doing an email send. """
    email_server = smtplib.SMTP("localhost")
    try:
        email_server.sendmail(source_email_address,
                              destination_email_addresses,
                              'Subject: %s\n\n%s' % (subject, message))
    except Exception:
        raise
    finally:
        email_server.quit()
