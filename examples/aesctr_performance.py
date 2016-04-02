import time
import base64

from pyoram.crypto.aesctr import AESCTR

print("\nTest Bulk")
#
# generate a 32-byte key
#
key = AESCTR.KeyGen(32)
print("Key: %s" % (base64.b64encode(key)))

#
# generate some plaintext
#
nblocks = 10000000
plaintext_numbytes = AESCTR.block_size * nblocks
print("Plaintext Size: %s MB"
      % (plaintext_numbytes * 1.0e-6))
# all zeros
plaintext = bytes(bytearray(plaintext_numbytes))

#
# time encryption
#
start_time = time.time()
ciphertext = AESCTR.Enc(key, plaintext)
stop_time = time.time()
print("Encryption Time: %.3fs (%.3f MB/s)"
      % (stop_time-start_time,
         (plaintext_numbytes * 1.0e-6) / (stop_time-start_time)))

#
# time decryption
#
start_time = time.time()
plaintext_decrypted = AESCTR.Dec(key, ciphertext)
stop_time = time.time()
print("Decryption Time: %.3fs (%.3f MB/s)"
      % (stop_time-start_time,
         (plaintext_numbytes * 1.0e-6) / (stop_time-start_time)))

assert plaintext_decrypted == plaintext
assert ciphertext != plaintext
# IND-CPA
assert AESCTR.Enc(key, plaintext) != ciphertext
# make sure the only difference is not in the IV
assert AESCTR.Enc(key, plaintext)[AESCTR.block_size:] \
    != ciphertext[AESCTR.block_size:]
assert len(plaintext) == \
    len(ciphertext) - AESCTR.block_size

del plaintext
del plaintext_decrypted
del ciphertext

print("\nTest Chunks")
#
# generate a 32-byte key
#
key = AESCTR.KeyGen(32)
print("Key: %s" % (base64.b64encode(key)))

#
# generate some plaintext
#
nblocks = 10000
blocksize = 16000
total_bytes = blocksize * nblocks
print("Plaintext Size: %s KB" % (blocksize * 1.0e-3))
print("Block Count: %s" % (nblocks))
print("Total: %s MB" % (total_bytes * 1.0e-6))
plaintext_blocks = [bytes(bytearray(blocksize))
                    for i in range(nblocks)]

#
# time encryption
#
start_time = time.time()
ciphertext_blocks = [AESCTR.Enc(key, b)
                     for b in plaintext_blocks]
stop_time = time.time()
print("Encryption Time: %.3fs (%.3f MB/s)"
      % (stop_time-start_time,
         (total_bytes * 1.0e-6) / (stop_time-start_time)))

#
# time decryption
#
start_time = time.time()
plaintext_decrypted_blocks = [AESCTR.Dec(key, c)
                              for c in ciphertext_blocks]
stop_time = time.time()
print("Decryption Time: %.3fs (%.3f MB/s)"
      % (stop_time-start_time,
         (total_bytes * 1.0e-6) / (stop_time-start_time)))
