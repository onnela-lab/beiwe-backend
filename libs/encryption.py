import json
import traceback
from os import urandom

from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from flask import request

from config.constants import ASYMMETRIC_KEY_LENGTH
from config.settings import IS_STAGING
from database.profiling_models import (DecryptionKeyError, EncryptionErrorMetadata,
    LineEncryptionError)
from database.study_models import Study
from libs.logging import log_error
from libs.security import Base64LengthException, decode_base64, encode_base64, PaddingException


class DecryptionKeyInvalidError(Exception): pass
class HandledError(Exception): pass
# class UnHandledError(Exception): pass  # for debugging
class InvalidIV(Exception): pass
class InvalidData(Exception): pass
class DefinitelyInvalidFile(Exception): pass


# The private keys are stored server-side (S3), and the public key is sent to the device.

################################################################################
################################# RSA ##########################################
################################################################################


def generate_key_pairing() -> (bytes, bytes):
    """Generates a public-private key pairing, returns tuple (public, private)"""
    private_key = RSA.generate(ASYMMETRIC_KEY_LENGTH)
    public_key = private_key.publickey()
    return public_key.exportKey(), private_key.exportKey()


def prepare_X509_key_for_java(exported_key) -> bytes:
    # This may actually be a PKCS8 Key specification.
    """ Removes all extraneous config (new lines and labels from a formatted key string,
    because this is how Java likes its key files to be formatted.
    (Y'know, not in accordance with the specification.  Because Java.) """
    return b"".join(exported_key.split(b'\n')[1:-1])


def get_RSA_cipher(key: bytes) -> RSA._RSAobj:
    return RSA.importKey(key)

    # pycryptodome: the following appears to be correct, but pycryptodome raises a decryption error.
    # RSA_key = RSA.importKey(key)
    # cipher = PKCS1_OAEP.new(RSA_key)
    # return cipher


# This function is only for use in debugging.
# def encrypt_rsa(blob, private_key):
#     return private_key.encrypt("blob of text", "literally anything")
#     """ 'blob of text' can be either a long or a string, we will use strings.
#         The second parameter must be entered... but it is ignored.  Really."""


################################################################################
################################# AES ##########################################
################################################################################


def encrypt_for_server(input_string: bytes, study_object_id: str) -> bytes:
    """
    Encrypts config using the ENCRYPTION_KEY, prepends the generated initialization vector.
    Use this function on an entire file (as a string).
    """
    if not isinstance(study_object_id, str):
        raise Exception(f"received non-string object {study_object_id}")
    encryption_key = Study.objects.get(object_id=study_object_id).encryption_key.encode()  # bytes
    iv = urandom(16)  # bytes
    return iv + AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).encrypt(input_string)


def decrypt_server(data: bytes, study_object_id: str) -> bytes:
    """ Decrypts config encrypted by the encrypt_for_server function. """
    if not isinstance(study_object_id, str):
        raise Exception(f"received non-string object {study_object_id}")

    encryption_key = Study.objects.filter(
        object_id=study_object_id
    ).values_list('encryption_key', flat=True).get().encode()
    iv = data[:16]
    data = data[16:]
    return AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).decrypt(data)


########################### User/Device Decryption #############################


def decrypt_device_file(patient_id, original_data: bytes, private_key_cipher, user) -> bytes:
    """ Runs the line-by-line decryption of a file encrypted by a device.
    This function is a special handler for iOS file uploads. """

    def create_line_error_db_entry(error_type):
        # declaring this inside decrypt device file to access its function-global variables
        if IS_STAGING:
            LineEncryptionError.objects.create(
                type=error_type,
                base64_decryption_key=private_key_cipher.decrypt(decoded_key),
                line=encode_base64(line),
                prev_line=encode_base64(file_data[i - 1] if i > 0 else ''),
                next_line=encode_base64(file_data[i + 1] if i < len(file_data) - 1 else ''),
                participant=user,
            )

    def create_decryption_key_error(an_traceback):
        DecryptionKeyError.objects.create(
                file_path=request.values['file_name'],
                contents=original_data.decode(),
                traceback=an_traceback,
                participant=user,
        )
    
    bad_lines = []
    error_types = []
    error_count = 0
    good_lines = []
    file_data = [line for line in original_data.split(b'\n') if line != b""]
    
    if not file_data:
        raise HandledError("The file had no data in it.  Return 200 to delete file from device.")
    
    # The following code is strange because of an unfortunate design design decision made quite
    # some time ago: the decryption key is encoded as base64 twice, once wrapping the output of the
    # RSA encryption, and once wrapping the AES decryption key.
    # The second of the two except blocks likely means that the device failed to write the encryption
    # key as the first line of the file, but it may be a valid (but undecryptable) line of the file.
    try:
        decoded_key = decode_base64(file_data[0])
    except (TypeError, IndexError, PaddingException, Base64LengthException) as decode_error:
        create_decryption_key_error(traceback.format_exc())
        raise DecryptionKeyInvalidError("invalid decryption key. %s" % decode_error)

    try:
        base64_key = private_key_cipher.decrypt(decoded_key)
        decrypted_key = decode_base64(base64_key)
    except (TypeError, IndexError, PaddingException, Base64LengthException) as decr_error:
        create_decryption_key_error(traceback.format_exc())
        raise DecryptionKeyInvalidError("invalid decryption key. %s" % decr_error)

    for i, line in enumerate(file_data):
        # we need to skip the first line (the decryption key), but need real index values in i
        if i == 0:
            continue
        
        if line is None:
            # this case causes weird behavior inside decrypt_device_line, so we test for it instead.
            error_count += 1
            create_line_error_db_entry(LineEncryptionError.LINE_IS_NONE)
            error_types.append(LineEncryptionError.LINE_IS_NONE)
            bad_lines.append(line)
            print("encountered empty line of data, ignoring.")
            continue
            
        try:
            good_lines.append(decrypt_device_line(patient_id, decrypted_key, line))
        except Exception as error_orig:
            error_string = str(error_orig)
            error_count += 1
            
            error_message = "There was an error in user decryption: "
            if isinstance(error_string, IndexError):
                error_message += "Something is wrong with data padding:\n\tline: %s" % line
                log_error(error_string, error_message)
                create_line_error_db_entry(LineEncryptionError.PADDING_ERROR)
                error_types.append(LineEncryptionError.PADDING_ERROR)
                bad_lines.append(line)
                continue

            if isinstance(error_string, TypeError) and decrypted_key is None:
                error_message += "The key was empty:\n\tline: %s" % line
                log_error(error_string, error_message)
                create_line_error_db_entry(LineEncryptionError.EMPTY_KEY)
                error_types.append(LineEncryptionError.EMPTY_KEY)
                bad_lines.append(line)
                continue

            ################### skip these errors ##############################
            if "unpack" in error_string:
                error_message += "malformed line of config, dropping it and continuing."
                log_error(error_string, error_message)
                create_line_error_db_entry(LineEncryptionError.MALFORMED_CONFIG)
                error_types.append(LineEncryptionError.MALFORMED_CONFIG)
                bad_lines.append(line)
                #the config is not colon separated correctly, this is a single
                # line error, we can just drop it.
                # implies an interrupted write operation (or read)
                continue
                
            if "Input strings must be a multiple of 16 in length" in error_string:
                error_message += "Line was of incorrect length, dropping it and continuing."
                log_error(error_string, error_message)
                create_line_error_db_entry(LineEncryptionError.INVALID_LENGTH)
                error_types.append(LineEncryptionError.INVALID_LENGTH)
                bad_lines.append(line)
                continue
                
            if isinstance(error_string, InvalidData):
                error_message += "Line contained no data, skipping: " + str(line)
                log_error(error_string, error_message)
                create_line_error_db_entry(LineEncryptionError.LINE_EMPTY)
                error_types.append(LineEncryptionError.LINE_EMPTY)
                bad_lines.append(line)
                continue
                
            if isinstance(error_string, InvalidIV):
                error_message += "Line contained no iv, skipping: " + str(line)
                log_error(error_string, error_message)
                create_line_error_db_entry(LineEncryptionError.IV_MISSING)
                error_types.append(LineEncryptionError.IV_MISSING)
                bad_lines.append(line)
                continue
                
            ##################### flip out on these errors #####################
            if 'AES key' in error_string:
                error_message += "AES key has bad length."
                create_line_error_db_entry(LineEncryptionError.AES_KEY_BAD_LENGTH)
                error_types.append(LineEncryptionError.AES_KEY_BAD_LENGTH)
                bad_lines.append(line)
            elif 'IV must be' in error_string:
                error_message += "iv has bad length."
                create_line_error_db_entry(LineEncryptionError.IV_BAD_LENGTH)
                error_types.append(LineEncryptionError.IV_BAD_LENGTH)
                bad_lines.append(line)
            elif 'Incorrect padding' in error_string:
                error_message += "base64 padding error, config is truncated."
                create_line_error_db_entry(LineEncryptionError.MP4_PADDING)
                error_types.append(LineEncryptionError.MP4_PADDING)
                bad_lines.append(line)
                # this is only seen in mp4 files. possibilities:
                #  upload during write operation.
                #  broken base64 conversion in the app
                #  some unanticipated error in the file upload
            else:
                # If none of the above errors happened, raise the error raw
                raise
            raise HandledError(error_message)
            # if any of them did happen, raise a HandledError to cease execution.
    
    if error_count:
        EncryptionErrorMetadata.objects.create(
            file_name=request.values['file_name'],
            total_lines=len(file_data),
            number_errors=error_count,
            # generator comprehension:
            error_lines=json.dumps( (str(line for line in bad_lines)) ),
            error_types=json.dumps(error_types),
            participant=user,
        )

    # join should be rather well optimized and not cause O(n^2) total memory copies
    return b"\n".join(good_lines)


def decrypt_device_line(patient_id, key, data: bytes) -> bytes:
    """ Config is expected to be 3 colon separated values.
        value 1 is the symmetric key, encrypted with the patient's public key.
        value 2 is the initialization vector for the AES CBC cipher.
        value 3 is the config, encrypted using AES CBC, with the provided key and iv. """
    iv, data = data.split(b":")
    iv = decode_base64(iv)  # handle non-ascii encoding garbage...
    data = decode_base64(data)
    if not data:
        raise InvalidData()
    if not iv:
        raise InvalidIV()
    try:
        decipherer = AES.new(key, mode=AES.MODE_CBC, IV=iv)
        decrypted = decipherer.decrypt(data)
    except Exception:
        if iv is None:
            len_iv = "None"
        else:
            len_iv = len(iv)
        if data is None:
            len_data = "None"
        else:
            len_data = len(data)
        if key is None:
            len_key = "None"
        else:
            len_key = len(key)
        # these print statements cause problems in getting encryption errors because the print
        # statement will print to an ascii formatted log file on the server, which causes
        # ascii encoding error.  Enable them for debugging only. (leave uncommented for Sentry.)
        # print("length iv: %s, length data: %s, length key: %s" % (len_iv, len_data, len_key))
        # print('%s %s %s' % (patient_id, key, data))
        raise

    # PKCS5 Padding: The last byte of the byte-string contains the number of bytes at the end of the
    # bytestring that are padding.  As string slicing in python are a copy operation we will
    # detect the fast-path case of no change so that we can skip it
    num_padding_bytes = decrypted[-1]
    if num_padding_bytes:
        decrypted = decrypted[0: -num_padding_bytes]

    return decrypted
