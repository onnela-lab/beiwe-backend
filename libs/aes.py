from os import urandom

from Cryptodome.Cipher import AES


def encrypt_for_server(data: bytes, encryption_key: bytes) -> bytearray:
    """ Encrypts data using the ENCRYPTION_KEY, prepends the generated initialization vector.
    Use this function on an entire file (as a bytes).
    
    Returns a bytearray: the ciphertext is written directly after the iv in a single allocation,
    instead of `iv + ciphertext` which allocates the ciphertext and then copies it. (boto3 accepts a
    bytearray as an upload Body without copying it.) """
    if not isinstance(encryption_key, bytes):
        raise Exception(f"received non-bytes object {type(encryption_key)}")
    if len(encryption_key) != 32:
        raise Exception(f"received encryption key with bad length: {len(encryption_key)}")
    iv: bytes = urandom(16)  # bytes
    output = bytearray(16 + len(data))
    output[:16] = iv
    AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).encrypt(data, output=memoryview(output)[16:])
    return output


def decrypt_server(data: bytes, encryption_key: bytes) -> bytes:
    """ Decrypts config encrypted by the encrypt_for_server function. """
    if not isinstance(encryption_key, bytes):
        raise Exception(f"received non-bytes object {type(encryption_key)}")
    iv = data[:16]
    ciphertext = memoryview(data)[16:]  # slicing a memoryview does not copy the data
    return AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).decrypt(ciphertext)


def decrypt_server_in_place(data: bytearray, encryption_key: bytes) -> bytearray:
    """ As decrypt_server, but overwrites the ciphertext in `data` with the plaintext and removes the
    iv from the front, returning `data`. No second copy is allocated, removing bytes
    from the front of a bytearray does not move the remaining data. """
    if not isinstance(encryption_key, bytes):
        raise Exception(f"received non-bytes object {type(encryption_key)}")
    if not isinstance(data, bytearray):
        raise TypeError(f"in-place decryption requires a bytearray, received {type(data)}")
    iv = bytes(data[:16])
    view = memoryview(data)
    ciphertext = view[16:]
    AES.new(encryption_key, AES.MODE_CFB, segment_size=8, IV=iv).decrypt(ciphertext, output=ciphertext)
    # a bytearray cannot be resized while a memoryview of it exists
    ciphertext.release()
    view.release()
    del data[:16]
    return data
