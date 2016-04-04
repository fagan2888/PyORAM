import os
import unittest

from pyoram.storage.encrypted_heap_storage import \
    EncryptedHeapStorage
from pyoram.crypto.aesctr import AESCTR

from six.moves import xrange

thisdir = os.path.dirname(os.path.abspath(__file__))

class TestEncryptedHeapStorage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._block_size = 25
        cls._blocks_per_bucket = 3
        cls._base = 4
        cls._height = 2
        cls._testfname = cls.__name__ + "_testfile.bin"
        cls._buckets = []
        cls._type_name = "file"
        cls._key = EncryptedHeapStorage.setup(
            key_size=AESCTR.key_sizes[-1],
            storage_name=cls._testfname,
            block_size=cls._block_size,
            height=cls._height,
            base=cls._base,
            blocks_per_bucket=cls._blocks_per_bucket,
            storage_type=cls._type_name,
            initialize=lambda i: bytes(bytearray([i]) * \
                                       cls._block_size * \
                                       cls._blocks_per_bucket),
            ignore_existing=True)

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls._testfname)
        except OSError:                                # pragma: no cover
            pass                                       # pragma: no cover
        pass

    def test_setup_fails(self):
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=1,
                storage_name=os.path.join(thisdir, "baselines", "exists.empty"),
                block_size=10,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=1,
                storage_name=os.path.join(thisdir, "baselines", "exists.empty"),
                block_size=10,
                storage_type=self._type_name,
                ignore_existing=False)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=1,
                storage_name="tmp",
                block_size=0,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=1,
                storage_name="tmp",
                block_size=1,
                block_count=1,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                height=1,
                blocks_per_bucket=1,
                storage_name="tmp",
                block_size=1,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                height=-1,
                blocks_per_bucket=1,
                storage_name="tmp",
                block_size=1,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                height=1,
                blocks_per_bucket=0,
                storage_name="tmp",
                block_size=1,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=1,
                storage_name="tmp",
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=0,
                storage_name="tmp",
                block_size=1,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=0,
                storage_name="tmp",
                block_size=1,
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=1,
                base=1,
                storage_name="tmp",
                block_size=1,
                storage_type=self._type_name)
        with self.assertRaises(TypeError):
            EncryptedHeapStorage.setup(
                key_size=AESCTR.key_sizes[-1],
                height=1,
                blocks_per_bucket=1,
                storage_name="tmp",
                block_size=1,
                storage_type=self._type_name,
                user_header_data=2)

    def test_setup(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        if os.path.exists(fname):
            os.remove(fname)                           # pragma: no cover
        bsize = 10
        height = 2
        blocks_per_bucket = 1
        user_header_data = b'a'
        key = EncryptedHeapStorage.setup(key_size=AESCTR.key_sizes[-1],
                                         height=height,
                                         blocks_per_bucket=blocks_per_bucket,
                                         storage_name=fname,
                                         block_size=bsize,
                                         user_header_data=user_header_data)
        with EncryptedHeapStorage(encryption_key=key,
                                  storage_name=fname,
                                  storage_type=self._type_name) as f:
            self.assertEqual(f.user_header_data, user_header_data)
            self.assertEqual(f.encryption_key, key)
            self.assertEqual(f.blocks_per_bucket, blocks_per_bucket)
            self.assertEqual(f.bucket_count, 2**(height+1) - 1)
            self.assertEqual(f.bucket_size, bsize * blocks_per_bucket)
            self.assertEqual(f.storage_name, fname)
        os.remove(fname)

    def test_init_noexists(self):
        self.assertEqual(not os.path.exists(self._testfname+"SDFSDFSDFSFSDFS"),
                         True)
        with self.assertRaises(IOError):
            with EncryptedHeapStorage(
                    encryption_key=self._key,
                    storage_name=self._testfname+"SDFSDFSDFSFSDFS",
                    storage_type=self._type_name) as f:
                pass                                   # pragma: no cover

    def test_init_exists(self):
        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname, 'rb') as f:
            databefore = f.read()
        with EncryptedHeapStorage(encryption_key=self._key,
                                  storage_name=self._testfname,
                                  storage_type=self._type_name) as f:
            encrypted_size = f.ciphertext_bucket_size * \
                             ((self._base**(self._height+1)) - 1)//(self._base-1)
            self.assertEqual(f.encryption_key, self._key)
            self.assertEqual(f.bucket_size,
                             self._block_size * self._blocks_per_bucket)
            self.assertEqual(f.bucket_count,
                             ((self._base**(self._height+1)) - 1)//(self._base-1))
            self.assertEqual(f.storage_name, self._testfname)
            self.assertEqual(f.user_header_data, bytes())
            self.assertNotEqual(self._block_size * self._blocks_per_bucket,
                                f.ciphertext_bucket_size)
        self.assertEqual(len(databefore) >= encrypted_size, True)
        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname, 'rb') as f:
            dataafter = f.read()
        self.assertEqual(databefore, dataafter)

if __name__ == "__main__":
    unittest.main()                                    # pragma: no cover
