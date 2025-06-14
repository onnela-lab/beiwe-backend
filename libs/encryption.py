# trunk-ignore-all(ruff/B904)
import json
from typing import Counter

from Cryptodome.Cipher import AES
from Cryptodome.PublicKey import RSA
from django.forms import ValidationError

from constants.security_constants import URLSAFE_BASE64_CHARACTERS
from constants.user_constants import ANDROID_API, IOS_API
from database.data_access_models import IOSDecryptionKey
from database.profiling_models import EncryptionErrorMetadata
from database.user_models_participant import Participant
from libs.sentry import send_sentry_warning
from libs.utils.base64_utils import (Base64LengthException, decode_base64, encode_base64,
    PaddingException)


class DecryptionKeyInvalidError(Exception): pass
class IosDecryptionKeyNotFoundError(Exception): pass
class IosDecryptionKeyDuplicateError(Exception): pass
class RemoteDeleteFileScenario(Exception): pass
class UnHandledError(Exception): pass  # for debugging
class InvalidIV(Exception): pass
class InvalidData(Exception): pass
class DefinitelyInvalidFile(Exception): pass
class DoNotCatchThisErrorType(BaseException): pass

# TODO: there is a circular import due to the database imports in this file and this file being
# imported in s3, forcing local s3 imports in various files.  Refactor and fix.


class LineEncryptionError:
    AES_KEY_BAD_LENGTH = "AES_KEY_BAD_LENGTH"
    EMPTY_KEY = "EMPTY_KEY"
    INVALID_LENGTH = "INVALID_LENGTH"
    IV_BAD_LENGTH = "IV_BAD_LENGTH"
    IV_MISSING = "IV_MISSING"
    LINE_EMPTY = "LINE_EMPTY"
    LINE_IS_NONE = b"LINE_IS_NONE"
    MALFORMED_CONFIG = "MALFORMED_CONFIG"
    MP4_PADDING = "MP4_PADDING"
    PADDING_ERROR = "PADDING_ERROR"

########################### User/Device Decryption #############################

ENABLE_DECRYPTION_LOG = False

def log(*args, **kwargs):
    if ENABLE_DECRYPTION_LOG:
        print(*args, **kwargs)


class DeviceDataDecryptor():
    
    def __init__(
            self,
            file_name: str,
            original_data: bytes,
            participant: Participant,
            ignore_existing_keys: bool = False,
            rsa_key: RSA.RsaKey = None
        ) -> None:
        
        # basic info
        self.file_name: str = file_name
        self.original_data: bytes = original_data
        self.participant: Participant = participant
        log(f"decrypting {len(self.original_data)//1024} KB for:", self.file_name)
        
        # storage and error tracking
        self.bad_lines: list[bytes] = []
        self.error_types: list[str] = []
        self.good_lines: list[bytes] = []
        self.error_count: int = 0
        self.line_index = None  # line index is index to files_list variable of the current line
        
        # decryption key extraction
        if rsa_key:
            self.private_key_cipher = rsa_key
        else:
            self.private_key_cipher = self.participant.get_private_key()
        
        self.file_lines = self.split_file()
        
        # error management includes external assets, attribute needs to be populated.
        # DON'T pre-populate self.decrypted_file
        self.used_ios_decryption_key_cache = False
        
        self.ignore_existing_keys = ignore_existing_keys
        
        # os determination and go
        if participant.os_type == ANDROID_API or ignore_existing_keys:
            self.do_normal_decryption()
        elif participant.os_type == IOS_API:
            self.do_decryption_with_key_checking()
        else:
            raise Exception(f"Unknown operating system: {participant.os_type}")
    
    def do_normal_decryption(self):
        """ Android has no exciting features, errors are raised as normal. """
        self.aes_decryption_key = self.extract_aes_key()
        self.decrypt_device_file()
        # join is optimized and does not cause O(n^2) total memory copies.
        self.decrypted_file = b"\n".join(self.good_lines)
    
    def do_decryption_with_key_checking(self):
        """ Old iOS app versions (before Jan 2024) can upload identically named files that were
        split in half (or more)? due to a bug (the iOS app is bad.) We stash keys of all uploaded
        ios data, and use them to decrypt these "duplicate" files. """
        try:
            self.aes_decryption_key = self.extract_aes_key()
        except DecryptionKeyInvalidError:
            if self.ignore_existing_keys:
                raise
            self.aes_decryption_key = self.get_backup_encryption_key()
            self.used_ios_decryption_key_cache = True
        
        self.decrypt_device_file()
        # join is optimized and does not cause O(n^2) total memory copies.
        self.decrypted_file = b"\n".join(self.good_lines)
    
    def get_backup_encryption_key(self):
        if self.ignore_existing_keys:
            raise DoNotCatchThisErrorType(" get_backup_encryption_key should not have been called with ignore_existing_keys==True")
        try:
            decryption_key = IOSDecryptionKey.objects.get(file_name=self.file_name)
        except IOSDecryptionKey.DoesNotExist:
            raise IosDecryptionKeyNotFoundError(
                f"ios decryption key for '{self.file_name}' could not be found."
            )
        
        return decode_base64(decryption_key.base64_encryption_key.encode())
    
    def split_file(self) -> list[bytes]:
        # don't refactor to pop the decryption key line out of the file_data list, this list
        # can be thousands of lines.  Also, this line is a 2x memcopy with N new bytes objects.
        file_data = [line for line in self.original_data.split(b'\n') if line != b""]
        if not file_data:
            raise RemoteDeleteFileScenario("The file had no data in it.  Return 200 to delete file from device.")
        return file_data
    
    def decrypt_device_file(self):
        """ Runs the line-by-line decryption of a file encrypted by a device. """
        self.basic_file_validation()  # ok but first blow up if this happens.
        
        # we need to skip the first line (the decryption key), but need real index values
        lines = enumerate(self.file_lines)
        next(lines)
        for line_index, line in lines:
            self.line_index = line_index
            if line is None:
                # this case causes weird behavior inside decrypt_device_line, so we test for it instead.
                self.error_count += 1
                self.record_line_error(LineEncryptionError.LINE_IS_NONE, line)
                # print("encountered empty line of data, ignoring.")
                continue
            try:
                self.good_lines.append(self.decrypt_device_line(line))
            except Exception as error_orig:
                self.handle_line_error(line, error_orig)
        self.conditionally_create_metadata_error()
    
    def basic_file_validation(self):
        # Test for all null bytes. Very occasionally this occurs as the result of unknown data
        # corruption sources that are probably the result of real-world data corruption, not code
        # bugs. Data is _base64 encoded_, there should be no null bytes, all null bytes is worse.
        if (null_count:= self.original_data.count(b"\00")) == len(self.original_data):
            send_sentry_warning(
                self.participant.os_type + " file was all null bytes.",
                file_name=self.file_name,
                participant_id=self.participant.id,
                participant_os=self.participant.os_type,
                byte_count=len(self.original_data),
            )
            raise RemoteDeleteFileScenario("The file was null bytes.")
        
        if null_count > 0:
            print("debugging null bytes start")
            
            print("file name:", self.file_name)
            print(f"there were {null_count} null bytes in the file out of {len(self.original_data)} bytes total.")
            
            no_nulls = self.original_data.replace(b"\00", b"")
            print("breakdown:", Counter(no_nulls))
            
            print("debugging null bytes end")
            
            # print(self.original_data)
            
            """
            after 24 hours
            May 15 18:33:23: there were 3350 null bytes in the file out of 157195 bytes total.
            May 15 18:33:23: there were 17018 null bytes in the file out of 785789 bytes total.
            May 16 05:40:41: there were 90 null bytes in the file out of 2229 bytes total.
            May 16 05:56:54: there were 90 null bytes in the file out of 2217 bytes total.
            May 16 05:59:07: there were 3752 null bytes in the file out of 133879 bytes total.
            May 16 06:01:53: there were 40200 null bytes in the file out of 924479 bytes total.
            """
    
    def extract_aes_key(self) -> bytes:
        """ The following code is a bit dumb. The decryption key is encoded as base64 twice,
        once to wrap output of the RSA encryption, and once wrapping the AES decryption key. 
        Code factoring is weird due to the need to create and preserve legible stack traces.
        ( traceback.format_exc() gets the current stack trace if there is an error.) """
        log("extract_aes_key start")
        try:
            key_base64_raw: bytes = self.file_lines[0]
        except IndexError:
            # shouldn't be reachable due to test for emptiness prior in code, keep around anyway.
            log("extract_aes_key fail 1")
            raise DecryptionKeyInvalidError("There was no decryption key.")
        
        # Test that every byte in the byte-string of the raw key is a valid url-safe base64
        # character this also cuts down some junk files.
        for c in key_base64_raw:
            if c not in URLSAFE_BASE64_CHARACTERS:
                log(f"extract_aes_key fail 2: '{key_base64_raw.decode()}' character: '{chr(c)}'")
                raise DecryptionKeyInvalidError(f"Key not base64 encoded: {str(key_base64_raw)}")
        
        # handle the various cases that can occur when extracting from base64.
        try:
            decoded_key: bytes = decode_base64(key_base64_raw)
        except (TypeError, PaddingException, Base64LengthException) as decode_error:
            log("extract_aes_key fail 3")
            raise DecryptionKeyInvalidError(f"Invalid decryption key: {decode_error}")
        
        base64_key = self.rsa_decrypt(decoded_key)
        
        try:
            decrypted_key: bytes = decode_base64(base64_key)
            if not decrypted_key:
                log("extract_aes_key fail 4")
                raise TypeError(f"decoded key was '{decrypted_key}'")
        except (TypeError, IndexError, PaddingException, Base64LengthException) as decr_error:
            log("extract_aes_key fail 5")
            raise DecryptionKeyInvalidError(f"Invalid decryption key: {decr_error}")
        
        # If the decoded bits of the key is not exactly 128 bits (16 bytes) that probably means that
        # the RSA encryption failed - this occurs when the first byte of the encrypted blob is all
        # zeros.  Apps require an update to solve this (in a future rewrite we should use a correct
        # padding algorithm).
        # March 2024: this can happen when you have the wrong RSA key.
        if len(decrypted_key) != 16:
            log("extract_aes_key 6")
            raise DecryptionKeyInvalidError(f"Decryption key not 128 bits: {decrypted_key}")
        
        if self.participant.os_type == IOS_API:
            self.populate_ios_decryption_key(base64_key)
        
        log("extract_aes_key success")
        return decrypted_key
    
    def rsa_decrypt(self, decoded_key: bytes) -> bytes:
        # TODO: populate with exception case handling
        # PyCryptodome deprecated the old PyCrypto method RSA.decrypt() which could decrypt
        # textbook/raw RSA without key padding, which is what the Android & iOS apps write. This
        # (github.com/Legrandin/pycryptodome/issues/434#issuecomment-660701725) presents a
        # plain-math implementation of RSA.decrypt(), which we use instead.
        ciphertext_int = int.from_bytes(decoded_key, 'big')
        plaintext_int = pow(ciphertext_int, self.private_key_cipher.d, self.private_key_cipher.n)
        # return base64_key
        return plaintext_int.to_bytes(self.private_key_cipher.size_in_bytes(), 'big').lstrip(b'\x00')
    
    def populate_ios_decryption_key(self, base64_key: bytes):
        """ iOS has a bug where the file gets split into two uploads, so the second one is missing a
        decryption key. We store iOS decryption keys. and use them for those files - because the ios
        app "resists analysis" (its bad. its just bad.)
        
        We also have to handle the case of double uploads leading to violating the unique database,
        constraint. (again, the ios app is bad.) """
        # case: the base64 encoding can come in garbled, but still pass through decode_base64 as an
        # un-unicodeable 256 byte(?!) binary blob, but it base64 decodes into a 16 byte key. The fix
        # is to decode_base64 -> encode_base64, which magically creates the correct base64 blob. wtf
        try:
            base64_str: str = base64_key.decode()
        except UnicodeDecodeError:
            # this error case makes no sense
            base64_str: str = encode_base64(decode_base64(base64_key)).decode()
        
        try:
            IOSDecryptionKey.objects.create(
                file_name=self.file_name,
                base64_encryption_key=base64_str,
                participant=self.participant,
            )
            return
        except ValidationError as e:
            log(f"ios key creation FAILED for '{self.file_name}'")
            
            if "already exists" not in str(e):  # don't fail on other validation errors
                raise
            
            extant_key: IOSDecryptionKey = IOSDecryptionKey.objects.get(file_name=self.file_name)
            # assert both keys are identical.
            if extant_key.base64_encryption_key != base64_str:
                log("ios key creation unknown error 2")
                raise IosDecryptionKeyDuplicateError(
                    f"Two files, same name, two keys: '{extant_key.file_name}': "
                    f"extant key: '{extant_key.base64_encryption_key}', '"
                    f"new key: '{base64_str}'"
                )
    
    def decrypt_device_line(self, base64_data: bytes) -> bytes:
        """ Config (the file and its iv; why I named it that is a mystery) is expected to be 3 colon
            separated values.
            value 1 is the symmetric key, encrypted with the patient's public key.
            value 2 is the initialization vector for the AES CBC cipher.
            value 3 is the config, encrypted using AES CBC, with the provided key and iv. """
        # this can fail if the line is missing or has extra :'s, the case is handled as line error
        iv, base64_data = base64_data.split(b":")
        iv = decode_base64(iv)
        raw_data = decode_base64(base64_data)
        
        # handle cases of no data, and less than 16 bytes of data, which is an equivalent scenario.
        if not raw_data or len(raw_data) < 16:
            raise InvalidData()
        if not iv or len(iv) < 16:
            raise InvalidIV()
        
        # CBC data encryption requires alignment to a 16 bytes, we lose any data that overflows that length.
        overflow_bytes = len(raw_data) % 16
        
        if overflow_bytes:
            # print("\n\nFOUND OVERFLOWED DATA\n\n")
            # print("device os:", self.participant.os_type)
            # print("\n\n")
            raw_data = raw_data[:-overflow_bytes]
        
        try:
            decipherer = AES.new(self.aes_decryption_key, mode=AES.MODE_CBC, IV=iv)
            decrypted = decipherer.decrypt(raw_data)
        except Exception:
            if iv is None:
                len_iv = "None"
            else:
                # trunk-ignore(ruff/F841)
                len_iv = len(iv)
            if raw_data is None:
                len_data = "None"
            else:
                # trunk-ignore(ruff/F841)
                len_data = len(raw_data)
            if self.aes_decryption_key is None:
                len_key = "None"
            else:
                # trunk-ignore(ruff/F841)
                len_key = len(self.aes_decryption_key)
            # these print statements cause problems in getting encryption errors because the print
            # statement will print to an ascii formatted log file on the server, which causes
            # ascii encoding error.  Enable them for debugging only. (leave uncommented for Sentry.)
            # print("length iv: %s, length data: %s, length key: %s" % (len_iv, len_data, len_key))
            # print('%s %s %s' % (patient_id, key, orig_data))
            raise
        
        # PKCS5 Padding: The last byte of the byte-string contains the number of bytes at the end of the
        # bytestring that are padding.  As string slicing in python are a copy operation we will
        # detect the fast-path case of no change so that we can skip it
        num_padding_bytes = decrypted[-1]
        if num_padding_bytes:
            decrypted = decrypted[0: -num_padding_bytes]
        return decrypted
    
    def handle_line_error(self, line: bytes, error: Exception):
        error_string = str(error)
        this_error_message = "There was an error in user decryption: "
        self.error_count += 1
        
        if isinstance(error, (Base64LengthException, PaddingException)):
            # this case used to also catch IndexError, this probably changed after python3 upgrade
            this_error_message += "Something is wrong with data padding:\n\tline: %s" % line
            self.record_line_error(line, LineEncryptionError.PADDING_ERROR)
            return
        # TODO: untested, error should be caught as a decryption key error
        # elif isinstance(error, ValueError) and "Key cannot be the null string" in error_string:
        #     this_error_message += "The key was the null string:\n\tline: %s" % line
        #     self.append_line_encryption_error(line, LineEncryptionError.EMPTY_KEY)
        #     return
        ################### skip these errors ##############################
        if "values to unpack" in error_string:
            # the config is not colon separated correctly, this is a single line error, we can just
            # drop it. implies an interrupted write operation (or read)
            this_error_message += "malformed line of config, dropping it and continuing."
            self.record_line_error(line, LineEncryptionError.MALFORMED_CONFIG)
            return
        if isinstance(error, InvalidData):
            this_error_message += "Line contained no data, skipping: " + str(line)
            self.record_line_error(line, LineEncryptionError.LINE_EMPTY)
            return
        
        if isinstance(error, InvalidIV):
            this_error_message += "Line contained no iv, skipping: " + str(line)
            self.record_line_error(line, LineEncryptionError.IV_MISSING)
            return
        elif "Incorrect IV length" in error_string or 'IV must be' in error_string:
            # shifted this to an okay-to-proceed line error March 2021
            # Jan 2022: encountered pycryptodome form: "Incorrect IV length"
            this_error_message += "iv has bad length."
            self.record_line_error(line, LineEncryptionError.IV_BAD_LENGTH)
            return
        elif 'Incorrect padding' in error_string:
            this_error_message += "base64 padding error, config is truncated."
            self.record_line_error(line, LineEncryptionError.MP4_PADDING)
            # this is only seen in mp4 files. possibilities: upload during write operation. broken
            #  base64 conversion in the app some unanticipated error in the file upload
            if not self.file_name.endswith(".csv"):
                raise RemoteDeleteFileScenario(this_error_message)
        
        # If none of the above cases returned or errors, raise the error raw.
        raise error
    
    def record_line_error(self, line: bytes, error_type: str):
        self.error_types.append(error_type)
        self.bad_lines.append(line)
    
    def conditionally_create_metadata_error(self):
        if self.error_count:
            EncryptionErrorMetadata.objects.create(
                file_name=self.file_name,
                total_lines=len(self.file_lines),
                number_errors=self.error_count,
                # get lines in the form of strings  that read """b'\x00\x00\x00\x00'"""
                error_lines=json.dumps([str(line) for line in self.bad_lines]),
                error_types=json.dumps(self.error_types),
                participant=self.participant,
            )
