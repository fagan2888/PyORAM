import time
import base64

from pyoram.crypto.aes import AES

def runtest(label, enc_func, dec_func):
    print("")
    print("$"*20)
    print("{0:^20}".format(label))
    print("$"*20)
    for keysize in AES.key_sizes[:3]:
        print("")
        print("@@@@@@@@@@@@@@@@@@@@")
        print(" Key Size: %s bytes" % (keysize))
        print("@@@@@@@@@@@@@@@@@@@@")
        print("\nTest Bulk")
        #
        # generate a key
        #
        key = AES.KeyGen(keysize)
        print("Key: %s" % (base64.b64encode(key)))

        #
        # generate some plaintext
        #
        nblocks = 1000000
        plaintext_numbytes = AES.block_size * nblocks
        print("Plaintext Size: %s MB"
              % (plaintext_numbytes * 1.0e-6))
        # all zeros
        plaintext = bytes(bytearray(plaintext_numbytes))

        #
        # time encryption
        #
        start_time = time.time()
        ciphertext = enc_func(key, plaintext)
        stop_time = time.time()
        print("Encryption Time: %.3fs (%.3f MB/s)"
              % (stop_time-start_time,
                 (plaintext_numbytes * 1.0e-6) / (stop_time-start_time)))

        #
        # time decryption
        #
        start_time = time.time()
        plaintext_decrypted = dec_func(key, ciphertext)
        stop_time = time.time()
        print("Decryption Time: %.3fs (%.3f MB/s)"
              % (stop_time-start_time,
                 (plaintext_numbytes * 1.0e-6) / (stop_time-start_time)))

        assert plaintext_decrypted == plaintext
        assert ciphertext != plaintext
        # IND-CPA
        assert enc_func(key, plaintext) != ciphertext
        # make sure the only difference is not in the IV
        assert enc_func(key, plaintext)[AES.block_size:] \
            != ciphertext[AES.block_size:]
        if enc_func is AES.CTREnc:
            assert len(plaintext) == \
                len(ciphertext) - AES.block_size
        else:
            assert enc_func is AES.GCMEnc
            assert len(plaintext) == \
                len(ciphertext) - 2*AES.block_size

        del plaintext
        del plaintext_decrypted
        del ciphertext

        print("\nTest Chunks")
        #
        # generate a key
        #
        key = AES.KeyGen(keysize)
        print("Key: %s" % (base64.b64encode(key)))

        #
        # generate some plaintext
        #
        nblocks = 1000
        blocksize = 16000
        total_bytes = blocksize * nblocks
        print("Block Size: %s KB" % (blocksize * 1.0e-3))
        print("Block Count: %s" % (nblocks))
        print("Total: %s MB" % (total_bytes * 1.0e-6))
        plaintext_blocks = [bytes(bytearray(blocksize))
                            for i in range(nblocks)]

        #
        # time encryption
        #
        start_time = time.time()
        ciphertext_blocks = [enc_func(key, b)
                             for b in plaintext_blocks]
        stop_time = time.time()
        print("Encryption Time: %.3fs (%.3f MB/s)"
              % (stop_time-start_time,
                 (total_bytes * 1.0e-6) / (stop_time-start_time)))

        #
        # time decryption
        #
        start_time = time.time()
        plaintext_decrypted_blocks = [dec_func(key, c)
                                      for c in ciphertext_blocks]
        stop_time = time.time()
        print("Decryption Time: %.3fs (%.3f MB/s)"
              % (stop_time-start_time,
                 (total_bytes * 1.0e-6) / (stop_time-start_time)))

def main():
    runtest("AES - CTR Mode", AES.CTREnc, AES.CTRDec)
    runtest("AES - GCM Mode", AES.GCMEnc, AES.GCMDec)

if __name__ == "__main__":
    main()                                             # pragma: no cover
