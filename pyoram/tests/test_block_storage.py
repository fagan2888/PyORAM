import os
import unittest

from pyoram.storage.block_storage import (BlockStorageFile,
                                          BlockStorageMMapFile,
                                          BlockStorageS3)

from six.moves import xrange

thisdir = os.path.dirname(os.path.abspath(__file__))

class _TestBlockStorage(object):

    _type = None

    @classmethod
    def setUpClass(cls):
        assert cls._type is not None
        cls._blocksize = 25
        cls._blockcount = 5
        cls._testfname = cls._type.__name__ + "_testfile.bin"
        cls._blocks = []
        cls._type.setup(cls._testfname,
                        cls._blocksize,
                        cls._blockcount,
                        ignore_existing=True)
        with open(cls._testfname, "r+b") as f:
            f.seek(cls._type._index_offset)
            for i in xrange(cls._blockcount):
                data = bytearray([i])*cls._blocksize
                f.write(bytes(data))
                cls._blocks.append(data)
            f.flush()

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls._testfname)
        except OSError:
            pass
        pass

    def test_setup_fails(self):
        with self.assertRaises(ValueError):
            self._type.setup(
                os.path.join(thisdir, "baselines", "exists.empty"),
                10,
                10)
        with self.assertRaises(ValueError):
            self._type.setup(
                os.path.join(thisdir, "baselines", "exists.empty"),
                10,
                10,
                ignore_existing=False)
        with self.assertRaises(ValueError):
            self._type.setup("tmp", 0, 1)
        with self.assertRaises(ValueError):
            self._type.setup("tmp", 1, 0)

    def test_setup(self):
        fname = ".".join(self.id().split(".")[1:])
        fname += ".bin"
        fname = os.path.join(thisdir, fname)
        if os.path.exists(fname):
            os.remove(fname)
        bsize = 10
        bcount = 11
        self._type.setup(fname, bsize, bcount)
        with self._type(fname) as f:
            self.assertEqual(f.blocksize, bsize)
            self.assertEqual(f.blockcount, bcount)
            self.assertEqual(f.filename, fname)
        os.remove(fname)

    def test_init_noexists(self):
        self.assertEqual(not os.path.exists(self._testfname+"SDFSDFSDFSFSDFS"),
                         True)
        with self.assertRaises(IOError):
            with self._type(self._testfname+"SDFSDFSDFSFSDFS") as f:
                pass

    def test_init_exists(self):
        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname) as f:
            databefore = f.read()
        with self._type(self._testfname) as f:
            self.assertEqual(f.blocksize, self._blocksize)
            self.assertEqual(f.blockcount, self._blockcount)
            self.assertEqual(f.filename, self._testfname)

        self.assertEqual(os.path.exists(self._testfname), True)
        with open(self._testfname) as f:
            dataafter = f.read()
        self.assertEqual(databefore, dataafter)

    def test_read_block(self):
        with self._type(self._testfname) as f:
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
        with self._type(self._testfname) as f:
            self.assertEqual(list(bytearray(f.read_block(0))),
                             list(self._blocks[0]))
            self.assertEqual(list(bytearray(f.read_block(self._blockcount-1))),
                             list(self._blocks[-1]))

    def test_write_block(self):
        data = bytearray([self._blockcount])*self._blocksize
        self.assertEqual(len(data) > 0, True)
        with self._type(self._testfname) as f:
            for i in xrange(self._blockcount):
                self.assertNotEqual(list(bytearray(f.read_block(i))),
                                    list(data))
            for i in xrange(self._blockcount):
                f.write_block(i, bytes(data))
            for i in xrange(self._blockcount):
                self.assertEqual(list(bytearray(f.read_block(i))),
                                 list(data))
            for i, block in enumerate(self._blocks):
                f.write_block(i, bytes(block))

    def test_read_blocks(self):
        with self._type(self._testfname) as f:
            data = f.read_blocks(list(xrange(self._blockcount)))
            self.assertEqual(len(data), self._blockcount)
            for i, block in enumerate(data):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            data = f.read_blocks([0])
            self.assertEqual(len(data), 1)
            self.assertEqual(list(bytearray(data[0])),
                             list(self._blocks[0]))
            self.assertEqual(len(self._blocks) > 1, True)
            data = f.read_blocks(list(xrange(1, self._blockcount)) + [0])
            self.assertEqual(len(data), self._blockcount)
            for i, block in enumerate(data[:-1], 1):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            self.assertEqual(list(bytearray(data[-1])),
                             list(self._blocks[0]))

    def test_write_blocks(self):
        data = [bytearray([self._blockcount])*self._blocksize
                for i in xrange(self._blockcount)]
        with self._type(self._testfname) as f:
            orig = f.read_blocks(list(xrange(self._blockcount)))
            self.assertEqual(len(orig), self._blockcount)
            for i, block in enumerate(orig):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))
            f.write_blocks(list(xrange(self._blockcount)),
                           [bytes(b) for b in data])
            new = f.read_blocks(list(xrange(self._blockcount)))
            self.assertEqual(len(new), self._blockcount)
            for i, block in enumerate(new):
                self.assertEqual(list(bytearray(block)),
                                 list(data[i]))
            f.write_blocks(list(xrange(self._blockcount)),
                           [bytes(b) for b in self._blocks])
            orig = f.read_blocks(list(xrange(self._blockcount)))
            self.assertEqual(len(orig), self._blockcount)
            for i, block in enumerate(orig):
                self.assertEqual(list(bytearray(block)),
                                 list(self._blocks[i]))

class TestBlockStorageFile(_TestBlockStorage,
                           unittest.TestCase):
    _type = BlockStorageFile

class TestBlockStorageMMapFile(_TestBlockStorage,
                               unittest.TestCase):
    _type = BlockStorageMMapFile


if __name__ == "__main__":
    unittest.main()                                    # pragma: no cover

    #BlockStorageS3.setup('oram.bin',
    #                     10, 10,
    #                     'jgn2xm7s268qxlcuabtq')
    #with BlockStorageS3('oram.bin',
    #                    'jgn2xm7s268qxlcuabtq') as f:
    #    print(f.blocksize)
    #    print(f.blockcount)
    #    print(f.filename)
