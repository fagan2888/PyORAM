import unittest

from pyoram.crypto.aesctr import AESCTR

class Test(unittest.TestCase):

    def test_KeyGen(self):
        self.assertEqual(len(AESCTR.key_sizes), 3)
        self.assertEqual(len(set(AESCTR.key_sizes)), 3)
        for keysize in AESCTR.key_sizes:
            key_list = []
            key_set = set()
            for i in range(10):
                k = AESCTR.KeyGen(keysize)
                self.assertEqual(len(k), keysize)
                key_list.append(k)
                key_set.add(k)
            self.assertEqual(len(key_list), 10)
            # make sure every key is unique
            self.assertEqual(len(key_list), len(key_set))

    def test_Enc_Dec_bytes(self):
        self._test_Enc_Dec(
            lambda i, size: bytes(bytearray([i]) * size))

    def test_Enc_Dec_bytearray(self):
        self._test_Enc_Dec(
            lambda i, size: bytearray([i]) * size)

    def _test_Enc_Dec(self, get_plaintext):
        blocksize_factor = [0.5, 1, 1.5, 2, 2.5]
        plaintext_blocks = []
        for i, f in enumerate(blocksize_factor):
            size = AESCTR.block_size * f
            size = int(round(size))
            if int(f) != f:
                assert (size % AESCTR.block_size) != 0
            plaintext_blocks.append(get_plaintext(i, size))

        assert len(AESCTR.key_sizes) > 0
        ciphertext_blocks = {}
        keys = {}
        for keysize in AESCTR.key_sizes:
            key = AESCTR.KeyGen(keysize)
            keys[keysize] = key
            ciphertext_blocks[keysize] = []
            for block in plaintext_blocks:
                ciphertext_blocks[keysize].append(
                    AESCTR.Enc(key, block))

        self.assertEqual(len(ciphertext_blocks),
                         len(AESCTR.key_sizes))
        self.assertEqual(len(keys),
                         len(AESCTR.key_sizes))

        plaintext_decrypted_blocks = {}
        for keysize in keys:
            key = keys[keysize]
            plaintext_decrypted_blocks[keysize] = []
            for block in ciphertext_blocks[keysize]:
                plaintext_decrypted_blocks[keysize].append(
                    AESCTR.Dec(key, block))

        self.assertEqual(len(plaintext_decrypted_blocks),
                         len(AESCTR.key_sizes))

        for i in range(len(blocksize_factor)):
            for keysize in AESCTR.key_sizes:
                self.assertEqual(
                    plaintext_blocks[i],
                    plaintext_decrypted_blocks[keysize][i])
                self.assertNotEqual(
                    plaintext_blocks[i],
                    ciphertext_blocks[keysize][i])
                self.assertEqual(
                    len(ciphertext_blocks[keysize][i]),
                    len(plaintext_blocks[i]) + AESCTR.block_size)
                # check IND-CPA
                key = keys[keysize]
                alt_ciphertext = AESCTR.Enc(key, plaintext_blocks[i])
                self.assertNotEqual(
                    ciphertext_blocks[keysize][i],
                    alt_ciphertext)
                self.assertEqual(
                    len(ciphertext_blocks[keysize][i]),
                    len(alt_ciphertext))
                self.assertNotEqual(
                    ciphertext_blocks[keysize][i][:AESCTR.block_size],
                    alt_ciphertext[:AESCTR.block_size])
                self.assertNotEqual(
                    ciphertext_blocks[keysize][i][AESCTR.block_size:],
                    alt_ciphertext[AESCTR.block_size:])

if __name__ == "__main__":
    unittest.main()                                    # pragma: no cover
