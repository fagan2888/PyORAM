__all__ = ("AESCTR",)

__all__ = ("AESCTR",)

import os
import cryptography.hazmat.primitives.ciphers
import cryptography.hazmat.backends

_backend = cryptography.hazmat.backends.default_backend()
_aes = cryptography.hazmat.primitives.ciphers.algorithms.AES
_cipher = cryptography.hazmat.primitives.ciphers.Cipher
_mode = cryptography.hazmat.primitives.ciphers.modes.CTR

class AESCTR(object):

    key_sizes = [k//8 for k in sorted(_aes.key_sizes)]
    block_size = _aes.block_size//8

    @staticmethod
    def KeyGen(size_bytes):
        assert size_bytes in AESCTR.key_sizes
        return os.urandom(size_bytes)

    @staticmethod
    def Enc(key, plaintext):
        iv = os.urandom(AESCTR.block_size)
        cipher = _cipher(_aes(key), _mode(iv), backend=_backend).encryptor()
        return iv + cipher.update(plaintext) + cipher.finalize()

    @staticmethod
    def Dec(key, ciphertext):
        iv = ciphertext[:AESCTR.block_size]
        cipher = _cipher(_aes(key), _mode(iv), backend=_backend).decryptor()
        return cipher.update(ciphertext[AESCTR.block_size:]) + cipher.finalize()
