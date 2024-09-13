# FILES IN UTILS SHOULD HAVE SPARSE IMPORTS SO THAT THEY CAN BE USED ANYWHERE.
# IF YOU ARE IMPORTING FROM A DATABASE MODEL YOU SHOULD PLACE IT ELSEWHERE. (ANNOTATION IMPORTS ARE OK)

import base64
from binascii import Error as base64_error


class Base64LengthException(Exception): pass
class PaddingException(Exception): pass


def encode_base64(data: bytes) -> bytes:
    """ Creates a base64 representation of an input string, strips all new lines. """
    return base64.urlsafe_b64encode(data).replace(b"\n", b"")


def decode_base64(data: bytes, paddiing_fix=0) -> bytes:
    """ unpacks url safe base64 encoded string. Throws a more obviously named variable when
    encountering a padding error, which just means that there was no base64 padding for base64
    blobs of invalid length (possibly invalid base64 ending characters). """
    try:
        return base64.urlsafe_b64decode(data)
    except base64_error as e:
        # (in python 3.8 the error message is changed to include this information.)
        length = len(data.strip(b"="))
        if length % 4 != 0:
            # stacktrace readers: this means corrupted data.
            raise Base64LengthException(
                f"Data provided had invalid length {length} after padding was removed."
            ) from e
        
        if "incorrect padding" in str(e).lower() or "number of data characters" in str(e).lower():
            # for unknown reasons sometimes the padding is wrong, probably on corrupted data.
            # Character counts supposed to be divisible by 4. recurring because its easy.
            if paddiing_fix <= 4:
                paddiing_fix += 1
                padding = b"=" * paddiing_fix
                return decode_base64(data + padding, paddiing_fix=paddiing_fix)
            # str(data) here is correct, we need a representation of the data, not the raw data.
            raise PaddingException(f'{str(e)} -- "{str(data)}"') from e
        
        raise  # preserves original stacktrace