from sys import version_info
from typing import Tuple

from Crypto.PublicKey import RSA as old_RSA
from Cryptodome.PublicKey import RSA

from constants.security_constants import ASYMMETRIC_KEY_LENGTH


# Pycrypto (not pycryptodome) uses an old function inside the std lib time library that was
# deprecated because the name is misleading.  The exact replacement is the process_time function,
# so we patch it to keep it working.
# TODO: We only use the old pycrypto because we are using a not-best-practice of the direct RSA
#   encryption instead of a something like PKCS1_OAEP (OAEP is a padding mechanism).  I have been
#   unable to replicate the old code (and have zero incentive to do so) using of either the
#   pycryptodome library (which explicitly disallows it) or the `rsa` library.
if version_info.minor > 7:
    import time
    time.clock = time.process_time


# The private keys are stored server-side (S3), and the public key is sent to the device.


def generate_key_pairing() -> Tuple[bytes, bytes]:
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


def get_RSA_cipher(key: bytes) -> old_RSA.RsaKey:
    return old_RSA.importKey(key)


# pycryptodome: the following is correct for PKCS1_OAEP.
# RSA_key = RSA.importKey(key)
# cipher = PKCS1_OAEP.new(RSA_key)
# return cipher

# This function is only for use in debugging.
# def encrypt_rsa(blob, private_key):
#     return private_key.encrypt("blob of text", "literally anything")
#     """ 'blob of text' can be either a long or a string, we will use strings.
#         The second parameter must be entered... but it is ignored.  Really."""
