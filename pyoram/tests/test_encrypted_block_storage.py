import os
import unittest

from pyoram.storage.encrypted_block_storage import \
    EncryptedBlockStorage
from pyoram.crypto.aesctr import AESCTR

from six.moves import xrange

thisdir = os.path.dirname(os.path.abspath(__file__))

class _TestEncryptedBlockStorage(object):

    _type_name = None

    @classmethod
    def setUpClass(cls):
        assert cls._type_name is not None
        cls._block_size = 25
        cls._block_count = 5
        cls._testfname = cls.__name__ + "_testfile.bin"
        cls._blocks = []
        f = EncryptedBlockStorage.setup(
            cls._testfname,
            cls._block_size,
            cls._block_count,
            key_size=AESCTR.key_sizes[-1],
            storage_type=cls._type_name,
            initialize=lambda i: bytes(bytearray([i])*cls._block_size),
            ignore_existing=True)
        f.close()
        cls._key = f.key
        for i in range(cls._block_count):
            data = bytearray([i])*cls._block_size
            cls._blocks.append(data)

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls._testfname)
        except OSError:                                # pragma: no cover
            pass                                       # pragma: no cover
        pass

    def test_setup_fails(self):
        dummy_name = "sdfsdfsldkfjwerwerfsdfsdfsd"
        self.assertEquals(os.path.exists(dummy_name), False)
        with self.assertRaises(ValueError):
            EncryptedBlockStorage.setup(
                os.path.join(thisdir,
                             "baselines",
                             "exists.empty"),
                block_size=10,
                block_count=10,
                key_size=AESCTR.key_sizes[-1],
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedBlockStorage.setup(
                os.path.join(thisdir,
                             "baselines",
                             "exists.empty"),
                block_size=10,
                block_count=10,
                key_size=AESCTR.key_sizes[-1],
                storage_type=self._type_name,
                ignore_existing=False)
        with self.assertRaises(ValueError):
            EncryptedBlockStorage.setup(
                dummy_name,
                block_size=0,
                block_count=1,
                key_size=AESCTR.key_sizes[-1],
                storage_type=self._type_name)
        with self.assertRaises(ValueError):
            EncryptedBlockStorage.setup(
                dummy_name,
                block_size=1,
                block_count=0,
                key_size=AESCTR.key_sizes[-1],
                storage_type=self._type_name)
        with self.assertRaises(TypeError):
            EncryptedBlockStorage.setup(
                dummy_name,
                block_size=1,
                block_count=1,
                key_size=AESCTR.key_sizes[-1],
                storage_type=self._type_name,
                user_header_data=2)

    def test_setup(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        if os.path.exists(fname):
            os.remove(fname)                           # pragma: no cover
        bsize = 10
        bcount = 11
        fsetup = EncryptedBlockStorage.setup(
            fname,
            block_size=bsize,
            block_count=bcount,
            key_size=AESCTR.key_sizes[-1])
        fsetup.close()
        with EncryptedBlockStorage(fname,
                                   key=fsetup.key,
                                   storage_type=self._type_name) as f:
            self.assertEqual(f.user_header_data, bytes())
            self.assertEqual(fsetup.user_header_data, bytes())
            self.assertEqual(f.key, fsetup.key)
            self.assertEqual(f.block_size, bsize)
            self.assertEqual(fsetup.block_size, bsize)
            self.assertEqual(f.block_count, bcount)
            self.assertEqual(fsetup.block_count, bcount)
            self.assertEqual(f.storage_name, fname)
            self.assertEqual(fsetup.storage_name, fname)
        os.remove(fname)

    def test_setup_withdata(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        if os.path.exists(fname):
            os.remove(fname)                           # pragma: no cover
        bsize = 10
        bcount = 11
        user_header_data = bytes(bytearray(['a',1,2]))
        fsetup = EncryptedBlockStorage.setup(
            fname,
            block_size=bsize,
            block_count=bcount,
            key_size=AESCTR.key_sizes[-1],
            user_header_data=user_header_data)
        fsetup.close()
        with EncryptedBlockStorage(fname,
                                   key=fsetup.key,
                                   storage_type=self._type_name) as f:
            self.assertEqual(f.user_header_data, user_header_data)
            self.assertEqual(fsetup.user_header_data, user_header_data)
            self.assertEqual(f.key, fsetup.key)
            self.assertEqual(f.block_size, bsize)
            self.assertEqual(fsetup.block_size, bsize)
            self.assertEqual(f.block_count, bcount)
            self.assertEqual(fsetup.block_count, bcount)
            self.assertEqual(f.storage_name, fname)
            self.assertEqual(fsetup.storage_name, fname)
        os.remove(fname)

    def test_init_noexists(self):
        self.assertEqual(not os.path.exists(self._testfname+"SDFSDFSDFSFSDFS"),
                         True)
        with self.assertRaises(IOError):
            with EncryptedBlockStorage(
                    self._testfname+"SDFSDFSDFSFSDFS",
                    key=self._key,
                    storage_type=self._type_name) as f:
                pass                                   # pragma: no cover

    def test_init_exists(self):
        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname, 'rb') as f:
            databefore = f.read()
        with EncryptedBlockStorage(self._testfname,
                                   key=self._key,
                                   storage_type=self._type_name) as f:
            encrypted_size = f.ciphertext_block_size * \
                             self._block_count
            self.assertEqual(f.key, self._key)
            self.assertEqual(f.block_size, self._block_size)
            self.assertEqual(f.block_count, self._block_count)
            self.assertEqual(f.storage_name, self._testfname)
            self.assertEqual(f.user_header_data, bytes())
            self.assertNotEqual(self._block_size,
                                f.ciphertext_block_size)
        self.assertEqual(len(databefore) >= encrypted_size, True)
        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname, 'rb') as f:
            dataafter = f.read()
        self.assertEqual(databefore, dataafter)

    def test_read_block(self):
        with EncryptedBlockStorage(self._testfname,
                                   key=self._key,
                                   storage_type=self._type_name) as f:
            for i, data in enumerate(self._blocks):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
            for i, data in enumerate(self._blocks):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
            for i, data in reversed(list(enumerate(self._blocks))):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
            for i, data in reversed(list(enumerate(self._blocks))):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(self._blocks[i]))
        with EncryptedBlockStorage(self._testfname,
                                   key=self._key,
                                   storage_type=self._type_name) as f:
            self.assertEqual(list(bytearray(f.read_block(0))),
                             list(self._blocks[0]))
            self.assertEqual(list(bytearray(f.read_block(self._block_count-1))),
                             list(self._blocks[-1]))

    def test_write_block(self):
        data = bytearray([self._block_count])*self._block_size
        self.assertEqual(len(data) > 0, True)
        with EncryptedBlockStorage(self._testfname,
                                   key=self._key,
                                   storage_type=self._type_name) as f:
            for i in xrange(self._block_count):
                self.assertNotEqual(list(bytearray(f.read_block(i))),
                                    list(data))
            for i in xrange(self._block_count):
                f.write_block(i, bytes(data))
            for i in xrange(self._block_count):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(data))
            for i, block in enumerate(self._blocks):
                f.write_block(i, bytes(block))

    def test_read_blocks(self):
        with EncryptedBlockStorage(self._testfname,
                                   key=self._key,
                                   storage_type=self._type_name) as f:
            data = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(data), self._block_count)
            for i, block in enumerate(data):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            data = f.read_blocks([0])
            self.assertEqual(len(data), 1)
            self.assertEqual(list(bytearray(data[0])),
                             list(self._blocks[0]))
            self.assertEqual(len(self._blocks) > 1, True)
            data = f.read_blocks(list(xrange(1, self._block_count)) + [0])
            self.assertEqual(len(data), self._block_count)
            for i, block in enumerate(data[:-1], 1):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            self.assertEqual(list(bytearray(data[-1])),
                             list(self._blocks[0]))

    def test_write_blocks(self):
        data = [bytearray([self._block_count])*self._block_size
                for i in xrange(self._block_count)]
        with EncryptedBlockStorage(self._testfname,
                                   key=self._key,
                                   storage_type=self._type_name) as f:
            orig = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(orig), self._block_count)
            for i, block in enumerate(orig):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            f.write_blocks(list(xrange(self._block_count)),
                           [bytes(b) for b in data])
            new = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(new), self._block_count)
            for i, block in enumerate(new):
                self.assertEqual(list(bytearray(block)),
                                 list(data[i]))
            f.write_blocks(list(xrange(self._block_count)),
                           [bytes(b) for b in self._blocks])
            orig = f.read_blocks(list(xrange(self._block_count)))
            self.assertEqual(len(orig), self._block_count)
            for i, block in enumerate(orig):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))

class TestEncryptedBlockStorageFile(_TestEncryptedBlockStorage,
                                    unittest.TestCase):
    _type_name = 'file'

class TestEncryptedBlockStorageMMapFile(_TestEncryptedBlockStorage,
                                        unittest.TestCase):
    _type_name = 'mmap'

if __name__ == "__main__":
    unittest.main()                                    # pragma: no cover
