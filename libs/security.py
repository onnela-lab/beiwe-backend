import base64, hashlib, re
import random

from flask import flash

from config.settings import FLASK_SECRET_KEY
from config.constants import ITERATIONS, PASSWORD_REQUIREMENT_REGEX_LIST
from config.study_constants import EASY_ALPHANUMERIC_CHARS

# pbkdf2 is a hashing protocol specifically for safe password hash generation.
from hashlib import pbkdf2_hmac as pbkdf2
from os import urandom


class DatabaseIsDownError(Exception): pass
class PaddingException(Exception): pass

def set_secret_key(app):
    """grabs the Flask secret key"""
    app.secret_key = FLASK_SECRET_KEY


################################################################################
################################## HASHING #####################################
################################################################################

# Mongo does not like strings with invalid binary config, so we store binary config
# using url safe base64 encoding.

def chunk_hash( data ):
    """ We need to hash data in a data stream chunk and store the hash in mongo. """
    return hashlib.md5( data ).digest().encode('base64')

def low_memory_chunk_hash( data ):
    """ as chunk_hash, but expects the object to contain an index-0 accessible string, which is
    passed by reference to reduce memory usage. """
    return hashlib.md5( data[0] ).digest().encode('base64')


def device_hash( data ):
    """ Hashes an input string using the sha256 hash, mimicing the hash used on
    the devices.  Expects a string not in base64, returns a base64 string."""
    sha256 = hashlib.sha256()
    sha256.update(data)
    return encode_base64( sha256.digest() )

def encode_base64(data):
    """ Creates a url safe base64 representation of an input string, strips
        new lines."""
    return base64.urlsafe_b64encode(data).replace("\n","")


def decode_base64(data):
    """ unpacks url safe base64 encoded string. """
    #there seemed to be some problems with inserting a config.encode('utf-8') here,
    # never determined why.
    try:
        return base64.urlsafe_b64decode(data)
    except TypeError as e:
        # print "this is the error:"
        # print e
        # print "this is the data:";
        # try:
        #     print data.encode("string_escape")
        # except Exception as e2:
        #     print "could not print data because of this error:", e2
        if "Incorrect padding" == e.message:
            raise PaddingException
        raise


def generate_user_hash_and_salt(password):
    """ Generates a hash and salt that will match a given input string, and also
        matches the hashing that is done on a user's device. 
        Input is anticipated to be any arbitrary string."""
    salt = encode_base64(urandom(16))
    password = device_hash(password)
    password_hashed = encode_base64(pbkdf2('sha1', password, salt, iterations=ITERATIONS, dklen=32))
    return password_hashed, salt


def generate_hash_and_salt(password):
    """ Generates a hash and salt that will match for a given input string.
        Input is anticipated to be any arbitrary string."""
    salt = encode_base64(urandom(16))
    password_hashed = encode_base64(pbkdf2('sha1', password, salt, iterations=ITERATIONS, dklen=32))
    return password_hashed, salt


def compare_password(proposed_password, salt, real_password_hash):
    """ Compares a proposed password with a salt and a real password, returns
        True if the hash results are identical.
        Expects the proposed password to be a base64 encoded string.
        Expects the real password to be a base64 encoded string. """
    proposed_hash = encode_base64(pbkdf2('sha1', proposed_password, salt, iterations=ITERATIONS, dklen=32))
    return proposed_hash == real_password_hash


def generate_user_password_and_salt():
    """ Generates a random password, and an associated hash and salt.
        The password is an uppercase alphanumeric string,
        the password hash and salt are base64 encoded strings. """
    password = generate_easy_alphanumeric_string()
    password_hash, salt = generate_user_hash_and_salt( password )
    return password, password_hash, salt


def generate_admin_password_and_salt():
    """ Generates a random password, and an associated hash and salt.
        The password is an uppercase alphanumeric string,
        the password hash and salt are base64 encoded strings. """
    password = generate_easy_alphanumeric_string()
    password_hash, salt = generate_hash_and_salt( password )
    return password, password_hash, salt


################################################################################
############################### Random #########################################
################################################################################

def generate_easy_alphanumeric_string():
    """
    Generates an "easy" alphanumeric (lower case) string of length 8 without the 0 (zero)
    character. This is a design decision, because users will have to type in the "easy"
    string on mobile devices, so we have made this a string that is easy to type and
    easy to distinguish the characters of (e.g. no I/l, 0/o/O confusion).
    """
    return ''.join(random.choice(EASY_ALPHANUMERIC_CHARS) for _ in range(8))


def generate_random_string():
    return hashlib.sha512( urandom(16) ).digest().encode('base64')


def check_password_requirements(password, flash_message=False):
    if len(password) < 8:
        if flash_message:
            flash("Your New Password must be at least 8 characters long.", "danger")
        return False
    for regex in PASSWORD_REQUIREMENT_REGEX_LIST:
        if not re.search(regex, password):
            if flash_message:
                flash("Your New Password must contain at least one symbol, one number, "
                      "one lowercase, and one uppercase character.", "danger")
            return False
    return True
