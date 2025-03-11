from os import urandom

from Cryptodome.Cipher import AES


# todo: use the buffer interface to make encryption/decryption faster/not use 2x memory (if possible)


def encrypt_for_server(data: bytes, encryption_key: bytes) -> bytes:
    """ Encrypts config using the ENCRYPTION_KEY, prepends the generated initialization vector.
    Use this function on an entire file (as a bytes). """
    if not isinstance(encryption_key, bytes):
        raise Exception(f"received non-bytes object {type(encryption_key)}")
    if len(encryption_key) != 32:
        raise Exception(f"received encryption key with bad length: {len(encryption_key)}")
    iv: bytes = urandom(16)  # bytes
    return iv + AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).encrypt(data)


def decrypt_server(data: bytes, encryption_key: bytes) -> bytes:
    """ Decrypts config encrypted by the encrypt_for_server function. """
    if not isinstance(encryption_key, bytes):
        raise Exception(f"received non-bytes object {type(encryption_key)}")
    iv = data[:16]
    data = data[16:]  # gr arg, memcopy operation...
    return AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).decrypt(data)
