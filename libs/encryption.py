import json
import traceback
from os import urandom
from typing import List

from Crypto.PublicKey import RSA
from Cryptodome.Cipher import AES
from flask import request

from config.constants import ASYMMETRIC_KEY_LENGTH
from config.settings import STORE_DECRYPTION_KEY_ERRORS, STORE_DECRYPTION_LINE_ERRORS
from constants.mobile_api import URLSAFE_BASE64_CHARACTERS
from database.profiling_models import (DecryptionKeyError, EncryptionErrorMetadata,
    LineEncryptionError)
from database.study_models import Study
from database.user_models import Participant
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
        raise TypeError(f"received non-string object {study_object_id}")

    encryption_key = Study.objects.filter(
        object_id=study_object_id
    ).values_list('encryption_key', flat=True).get().encode()
    iv = data[:16]
    data = data[16:]  # gr arg, memcopy operation...
    return AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).decrypt(data)


########################### User/Device Decryption #############################


def decrypt_device_file(original_data: bytes, participant: Participant) -> bytes:
    """ Runs the line-by-line decryption of a file encrypted by a device.  """

    def create_line_error_db_entry(error_type):
        # declaring this inside decrypt device file to access its function-global variables
        if STORE_DECRYPTION_LINE_ERRORS:
            LineEncryptionError.objects.create(
                type=error_type,
                base64_decryption_key=encode_base64(aes_decryption_key),
                line=encode_base64(line),
                prev_line=encode_base64(file_data[i - 1] if i > 0 else ''),
                next_line=encode_base64(file_data[i + 1] if i < len(file_data) - 1 else ''),
                participant=participant,
            )

    bad_lines = []
    error_types = []
    error_count = 0
    good_lines = []

    # don't refactor to pop the decryption key line out of the file_data list, this list
    # can be thousands of lines.  Also, this line is a 2x memcopy with N new bytes objects.
    file_data = [line for line in original_data.split(b'\n') if line != b""]

    if not file_data:
        raise HandledError("The file had no data in it.  Return 200 to delete file from device.")

    private_key_cipher = participant.get_private_key()
    aes_decryption_key = extract_aes_key(file_data, participant, private_key_cipher, original_data)

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
            good_lines.append(decrypt_device_line(participant.patient_id, aes_decryption_key, line))
        except Exception as error_orig:
            error_string = str(error_orig)
            error_count += 1

            error_message = "There was an error in user decryption: "
            if isinstance(error_orig, (Base64LengthException, PaddingException)):
                # this case used to also catch IndexError, this probably changed after python3 upgrade
                error_message += "Something is wrong with data padding:\n\tline: %s" % line
                create_line_error_db_entry(LineEncryptionError.PADDING_ERROR)
                error_types.append(LineEncryptionError.PADDING_ERROR)
                bad_lines.append(line)
                continue

            # case not reachable, decryption key has validation logic.
            # if isinstance(error_orig, TypeError) and aes_decryption_key is None:
            #     error_message += "The key was empty:\n\tline: %s" % line
            #     create_line_error_db_entry(LineEncryptionError.EMPTY_KEY)
            #     error_types.append(LineEncryptionError.EMPTY_KEY)
            #     bad_lines.append(line)
            #     continue

            # untested, error should be caught as a decryption key error
            # if isinstance(error_orig, ValueError) and "Key cannot be the null string" in error_string:
            #     error_message += "The key was the null string:\n\tline: %s" % line
            #     create_line_error_db_entry(LineEncryptionError.EMPTY_KEY)
            #     error_types.append(LineEncryptionError.EMPTY_KEY)
            #     bad_lines.append(line)
            #     continue

            ################### skip these errors ##############################
            if "unpack" in error_string:
                error_message += "malformed line of config, dropping it and continuing."
                create_line_error_db_entry(LineEncryptionError.MALFORMED_CONFIG)
                error_types.append(LineEncryptionError.MALFORMED_CONFIG)
                bad_lines.append(line)
                # the config is not colon separated correctly, this is a single
                # line error, we can just drop it.
                # implies an interrupted write operation (or read)
                continue

            if "Input strings must be a multiple of 16 in length" in error_string:
                error_message += "Line was of incorrect length, dropping it and continuing."
                create_line_error_db_entry(LineEncryptionError.INVALID_LENGTH)
                error_types.append(LineEncryptionError.INVALID_LENGTH)
                bad_lines.append(line)
                continue

            if isinstance(error_orig, InvalidData):
                error_message += "Line contained no data, skipping: " + str(line)
                create_line_error_db_entry(LineEncryptionError.LINE_EMPTY)
                error_types.append(LineEncryptionError.LINE_EMPTY)
                bad_lines.append(line)
                continue

            if isinstance(error_orig, InvalidIV):
                error_message += "Line contained no iv, skipping: " + str(line)
                create_line_error_db_entry(LineEncryptionError.IV_MISSING)
                error_types.append(LineEncryptionError.IV_MISSING)
                bad_lines.append(line)
                continue

            elif 'IV must be' in error_string:
                # shifted this to an okay-to-proceed line error March 2021.
                error_message += "iv has bad length."
                create_line_error_db_entry(LineEncryptionError.IV_BAD_LENGTH)
                error_types.append(LineEncryptionError.IV_BAD_LENGTH)
                bad_lines.append(line)
                continue

            # Give up on these errors:
            # should be handled in decryption key validation.
            # if 'AES key' in error_string:
            #     error_message += "AES key has bad length."
            #     create_line_error_db_entry(LineEncryptionError.AES_KEY_BAD_LENGTH)
            #     error_types.append(LineEncryptionError.AES_KEY_BAD_LENGTH)
            #     bad_lines.append(line)
            #     raise HandledError(error_message)

            elif 'Incorrect padding' in error_string:
                error_message += "base64 padding error, config is truncated."
                create_line_error_db_entry(LineEncryptionError.MP4_PADDING)
                error_types.append(LineEncryptionError.MP4_PADDING)
                bad_lines.append(line)
                # this is only seen in mp4 files. possibilities:
                #  upload during write operation.
                #  broken base64 conversion in the app
                #  some unanticipated error in the file upload
                raise HandledError(error_message)
            else:
                # If none of the above errors happened, raise the error raw
                raise

    if error_count:
        EncryptionErrorMetadata.objects.create(
            file_name=request.values['file_name'],
            total_lines=len(file_data),
            number_errors=error_count,
            # generator comprehension:
            error_lines=json.dumps( (str(line for line in bad_lines)) ),
            error_types=json.dumps(error_types),
            participant=participant,
        )

    # join should be rather well optimized and not cause O(n^2) total memory copies
    return b"\n".join(good_lines)


def extract_aes_key(
        file_data: List[bytes], participant: Participant, private_key_cipher, original_data: bytes
) -> bytes:
    # The following code is ... strange because of an unfortunate design design decision made
    # quite some time ago: the decryption key is encoded as base64 twice, once wrapping the
    # output of the RSA encryption, and once wrapping the AES decryption key.  This happened
    # because I was not an experienced developer at the time, python2's unified string-bytes
    # class didn't exactly help, and java io is... java io.

    def create_decryption_key_error(an_traceback):
        # helper function with local variable access.
        # do not refactor to include raising the error in this function, that obfuscates the source.
        if STORE_DECRYPTION_KEY_ERRORS:
            DecryptionKeyError.objects.create(
                    file_path=request.values['file_name'],
                    contents=original_data.decode(),
                    traceback=an_traceback,
                    participant=participant,
            )

    try:
        key_base64_raw: bytes = file_data[0]
        # print(f"key_base64_raw: {key_base64_raw}")
    except IndexError:
        # probably not reachable due to test for emptiness prior in code; keep just in case...
        create_decryption_key_error(traceback.format_exc())
        raise DecryptionKeyInvalidError("There was no decryption key.")

    # Test that every "character" (they are 8 bit bytes) in the byte-string of the raw key is
    # a valid url-safe base64 character, this will cut out certain junk files too.
    for c in key_base64_raw:
        if c not in URLSAFE_BASE64_CHARACTERS:
            # need a stack trace....
            try:
                raise DecryptionKeyInvalidError(f"Decryption key not base64 encoded: {key_base64_raw}")
            except DecryptionKeyInvalidError:
                create_decryption_key_error(traceback.format_exc())
                raise

    # handle the various cases that can occur when extracting from base64.
    try:
        decoded_key: bytes = decode_base64(key_base64_raw)
        # print(f"decoded_key: {decoded_key}")
    except (TypeError, PaddingException, Base64LengthException) as decode_error:
        create_decryption_key_error(traceback.format_exc())
        raise DecryptionKeyInvalidError(f"Invalid decryption key: {decode_error}")

    try:
        base64_key = private_key_cipher.decrypt(decoded_key)
        # print(f"base64_key: {len(base64_key)} {base64_key}")
        decrypted_key = decode_base64(base64_key)
        # print(f"decrypted_key: {len(decrypted_key)} {decrypted_key}")
        if not decrypted_key:
            raise TypeError(f"decoded key was '{decrypted_key}'")
    except (TypeError, IndexError, PaddingException, Base64LengthException) as decr_error:
        create_decryption_key_error(traceback.format_exc())
        raise DecryptionKeyInvalidError(f"Invalid decryption key: {decr_error}")

    # If the decoded bits of the key is not exactly 128 bits (16 bytes) that probably means that
    # the RSA encryption failed - this occurs when the first byte of the encrypted blob is all
    # zeros.  Apps require an update to solve this (in a future rewrite we should use a padding
    # algorithm).
    if len(decrypted_key) != 16:
        # print(len(decrypted_key))
        # need a stack trace....
        try:
            raise DecryptionKeyInvalidError(f"Decryption key not 128 bits: {decrypted_key}")
        except DecryptionKeyInvalidError:
            create_decryption_key_error(traceback.format_exc())
            raise

    return decrypted_key


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
