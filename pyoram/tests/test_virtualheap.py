import unittest

import pyoram
from pyoram.tree.virtualheap import VirtualHeap

class Test(unittest.TestCase):

    def test_MaxKLabeled(self):
        VirtualHeap.MaxKLabeled()

    def test_k(self):
        vh = VirtualHeap(2, 3, bucket_size=4)
        self.assertEqual(vh.k, 2)
        vh = VirtualHeap(5, 6, bucket_size=7)
        self.assertEqual(vh.k, 5)

    def test_Levels(self):
        vh = VirtualHeap(2, 3, bucket_size=4)
        self.assertEqual(vh.Levels(), 4)
        vh = VirtualHeap(5, 6, bucket_size=7)
        self.assertEqual(vh.Levels(), 7)

    def test_Height(self):
        vh = VirtualHeap(2, 3, bucket_size=4)
        self.assertEqual(vh.Height(), 3)
        vh = VirtualHeap(5, 6, bucket_size=7)
        self.assertEqual(vh.Height(), 6)

    def test_NodeLabelToBucket(self):
        vh = VirtualHeap(2, 4)
        self.assertEqual(vh.NodeLabelToBucket(""), 0)
        self.assertEqual(vh.NodeLabelToBucket("0"), 1)
        self.assertEqual(vh.NodeLabelToBucket("1"), 2)
        self.assertEqual(vh.NodeLabelToBucket("00"), 3)
        self.assertEqual(vh.NodeLabelToBucket("01"), 4)
        self.assertEqual(vh.NodeLabelToBucket("10"), 5)
        self.assertEqual(vh.NodeLabelToBucket("11"), 6)
        self.assertEqual(vh.NodeLabelToBucket("000"), 7)
        self.assertEqual(vh.NodeLabelToBucket("001"), 8)
        self.assertEqual(vh.NodeLabelToBucket("010"), 9)
        self.assertEqual(vh.NodeLabelToBucket("011"), 10)
        self.assertEqual(vh.NodeLabelToBucket("100"), 11)
        self.assertEqual(vh.NodeLabelToBucket("101"), 12)
        self.assertEqual(vh.NodeLabelToBucket("110"), 13)
        self.assertEqual(vh.NodeLabelToBucket("111"), 14)
        self.assertEqual(vh.NodeLabelToBucket("0000"), 15)
        self.assertEqual(vh.NodeLabelToBucket("1111"), vh.BucketCount()-1)

        vh = VirtualHeap(3, 3)
        self.assertEqual(vh.NodeLabelToBucket(""), 0)
        self.assertEqual(vh.NodeLabelToBucket("0"), 1)
        self.assertEqual(vh.NodeLabelToBucket("1"), 2)
        self.assertEqual(vh.NodeLabelToBucket("2"), 3)
        self.assertEqual(vh.NodeLabelToBucket("00"), 4)
        self.assertEqual(vh.NodeLabelToBucket("01"), 5)
        self.assertEqual(vh.NodeLabelToBucket("02"), 6)
        self.assertEqual(vh.NodeLabelToBucket("10"), 7)
        self.assertEqual(vh.NodeLabelToBucket("11"), 8)
        self.assertEqual(vh.NodeLabelToBucket("12"), 9)
        self.assertEqual(vh.NodeLabelToBucket("20"), 10)
        self.assertEqual(vh.NodeLabelToBucket("21"), 11)
        self.assertEqual(vh.NodeLabelToBucket("22"), 12)
        self.assertEqual(vh.NodeLabelToBucket("000"), 13)
        self.assertEqual(vh.NodeLabelToBucket("222"), vh.BucketCount()-1)

        for k in range(2, VirtualHeap.MaxKLabeled()+1):
            for height in range(5):
                vh = VirtualHeap(k, height)
                largest_symbol = vh.numerals[k-1]
                self.assertEqual(vh.k, k)
                self.assertEqual(vh.NodeLabelToBucket(""), 0)
                self.assertEqual(vh.NodeLabelToBucket(largest_symbol*height),
                                 vh.BucketCount()-1)

    def test_BucketSize(self):
        vh = VirtualHeap(2, 3, bucket_size=4)
        self.assertEqual(vh.BucketSize(), 4)
        vh = VirtualHeap(5, 6, bucket_size=7)
        self.assertEqual(vh.BucketSize(), 7)

    def test_ObjectCount(self):
        bases = list(range(2, 15)) + [VirtualHeap.MaxKLabeled()+1]
        for k in bases:
            for height in range(k+2):
                for bucket_size in range(1, 5):
                    vh = VirtualHeap(k, height, bucket_size=bucket_size)
                    cnt = (((k**(height+1))-1)//(k-1))
                    self.assertEqual(vh.BucketCount(), cnt)
                    self.assertEqual(vh.NodeCount(), cnt)
                    self.assertEqual(vh.SlotCount(), cnt * bucket_size)

    def test_ObjectCountAtLevel(self):
        bases = list(range(2, 15)) + [VirtualHeap.MaxKLabeled()+1]
        for k in bases:
            for height in range(k+2):
                for bucket_size in range(1, 5):
                    vh = VirtualHeap(k, height, bucket_size=bucket_size)
                    for l in range(height+1):
                        cnt = k**l
                        self.assertEqual(vh.BucketCountAtLevel(l), cnt)
                        self.assertEqual(vh.NodeCountAtLevel(l), cnt)
                        self.assertEqual(vh.SlotCountAtLevel(l), cnt * bucket_size)

    def test_LeafObjectCount(self):
        bases = list(range(2, 15)) + [VirtualHeap.MaxKLabeled()+1]
        for k in bases:
            for height in range(k+2):
                for bucket_size in range(1, 5):
                    vh = VirtualHeap(k, height, bucket_size=bucket_size)
                    self.assertEqual(vh.LeafBucketCount(),
                                     vh.BucketCountAtLevel(vh.Height()))
                    self.assertEqual(vh.LeafNodeCount(),
                                     vh.NodeCountAtLevel(vh.Height()))
                    self.assertEqual(vh.LeafSlotCount(),
                                     vh.SlotCountAtLevel(vh.Height()))

if __name__ == "__main__":
    unittest.main() # pragma: no cover
