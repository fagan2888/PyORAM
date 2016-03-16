import unittest

import pyoram.util

class Test(unittest.TestCase):

    def test_log2floor(self):
        self.assertEqual(pyoram.util.log2floor(1), 0)
        self.assertEqual(pyoram.util.log2floor(2), 1)
        self.assertEqual(pyoram.util.log2floor(3), 1)
        self.assertEqual(pyoram.util.log2floor(4), 2)
        self.assertEqual(pyoram.util.log2floor(5), 2)
        self.assertEqual(pyoram.util.log2floor(6), 2)
        self.assertEqual(pyoram.util.log2floor(7), 2)
        self.assertEqual(pyoram.util.log2floor(8), 3)
        self.assertEqual(pyoram.util.log2floor(9), 3)

    def test_log2ceil(self):
        self.assertEqual(pyoram.util.log2ceil(1), 0)
        self.assertEqual(pyoram.util.log2ceil(2), 1)
        self.assertEqual(pyoram.util.log2ceil(3), 2)
        self.assertEqual(pyoram.util.log2ceil(4), 2)
        self.assertEqual(pyoram.util.log2ceil(5), 3)
        self.assertEqual(pyoram.util.log2ceil(6), 3)
        self.assertEqual(pyoram.util.log2ceil(7), 3)
        self.assertEqual(pyoram.util.log2ceil(8), 3)
        self.assertEqual(pyoram.util.log2ceil(9), 4)

    def test_intdivceil(self):

        with self.assertRaises(ZeroDivisionError):
            pyoram.util.intdivceil(0, 0)
        with self.assertRaises(ZeroDivisionError):
            pyoram.util.intdivceil(1, 0)

        self.assertEqual(pyoram.util.intdivceil(1, 1), 1)
        self.assertEqual(pyoram.util.intdivceil(2, 3), 1)
        self.assertEqual(2 // 3, 0)
        self.assertEqual(pyoram.util.intdivceil(
            123123123123123123123123123123123123123123123123,
            123123123123123123123123123123123123123123123123), 1)
        self.assertEqual(pyoram.util.intdivceil(
            2 * 123123123123123123123123123123123123123123123123,
            123123123123123123123123123123123123123123123123), 2)
        self.assertEqual(pyoram.util.intdivceil(
            2 * 123123123123123123123123123123123123123123123123 + 1,
            123123123123123123123123123123123123123123123123), 3)
        self.assertEqual(pyoram.util.intdivceil(
            2 * 123123123123123123123123123123123123123123123123 - 1,
            123123123123123123123123123123123123123123123123), 2)
        self.assertEqual(
            (2 * 123123123123123123123123123123123123123123123123 - 1) // \
            123123123123123123123123123123123123123123123123,
            1)

if __name__ == "__main__":
    unittest.main()                                    # pragma: no cover
