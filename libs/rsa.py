from Cryptodome.PublicKey import RSA

from constants.security_constants import ASYMMETRIC_KEY_LENGTH


# RsaKey.__repr__ includes the private key components (d, p, q, u), and Sentry error reports
# include the repr() of local variables in stack frames. This module is imported by the Participant
# model, so the patch is applied in any process that can load a key.
def _sanitized_rsa_key_repr(self: RSA.RsaKey) -> str:
    return "<SANITIZED RsaKey>"

RSA.RsaKey.__repr__ = _sanitized_rsa_key_repr  # type: ignore[method-assign]


# The private keys are stored server-side (S3), and the public key is sent to the device.


def generate_key_pairing() -> tuple[bytes, bytes]:
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


def get_RSA_cipher(secret: bytes) -> RSA.RsaKey:
    # the name `secret` triggers sentry scrubbing, this can be a private key file.
    return RSA.importKey(secret)


################################################################################
######################### Client Key Management ################################
################################################################################


def create_participant_key_pair(patient_id: str, study_id: str):
    """Generate key pairing, push to database, return sanitized key for client."""
    from libs.s3 import s3_upload
    public, private = generate_key_pairing()
    s3_upload("keys/" + patient_id + "_private", private, study_id)
    s3_upload("keys/" + patient_id + "_public", public, study_id)


def get_participant_public_key_string(patient_id: str, study_id: str) -> str:
    """Grabs a user's public key string from s3."""
    from libs.s3 import s3_retrieve
    key_string = s3_retrieve("keys/" + patient_id + "_public", study_id)
    return prepare_X509_key_for_java(key_string).decode()


def get_participant_public_key(patient_id: str, study_id: str) -> RSA.RsaKey:
    """Grabs a user's public key file from s3."""
    from libs.s3 import s3_retrieve
    key = s3_retrieve("keys/" + patient_id + "_public", study_id)
    return get_RSA_cipher(key)


def get_participant_private_key(patient_id: str, study_id: str) -> RSA.RsaKey:
    """Grabs a user's private key file from s3."""
    from libs.s3 import s3_retrieve
    private_key = s3_retrieve("keys/" + patient_id + "_private", study_id)  # name triggers sentry scrubbing
    return get_RSA_cipher(private_key)


# pycryptodome: the following is correct for PKCS1_OAEP.
# RSA_key = RSA.importKey(key)
# cipher = PKCS1_OAEP.new(RSA_key)
# return cipher

# This function is only for use in debugging.
# def encrypt_rsa(blob, private_key):
#     return private_key.encrypt("blob of text", "literally anything")
#     """ 'blob of text' can be either a long or a string, we will use strings.
#         The second parameter must be entered... but it is ignored.  Really."""
