import unittest

import pyoram
from pyoram.tree.virtualheap import (VirtualHeap,
                                     VirtualHeapNode)

_test_bases = list(range(2, 15)) + [VirtualHeap.MaxKLabeled()+1]
_test_labeled_bases = list(range(2, 15)) + [VirtualHeap.MaxKLabeled()]

def _do_preorder(x):
    if x.level > 2:
        return
    yield x.bucket
    for c in range(x.k):
        for b in _do_preorder(x.ChildNode(c)):
            yield b

def _do_postorder(x):
    if x.level > 2:
        return
    for c in range(x.k):
        for b in _do_postorder(x.ChildNode(c)):
            yield b
    yield x.bucket

def _do_inorder(x):
    assert x.k == 2
    if x.level > 2:
        return
    for b in _do_inorder(x.ChildNode(0)):
        yield b
    yield x.bucket
    for b in _do_inorder(x.ChildNode(1)):
        yield b

class TestVirtualHeapNode(unittest.TestCase):

    def test_init(self):
        for k in _test_bases:
            for height in range(k+2):
                node = VirtualHeapNode(k, height)

    def test_level(self):
        self.assertEqual(VirtualHeapNode(2, 0).level, 0)
        self.assertEqual(VirtualHeapNode(2, 1).level, 1)
        self.assertEqual(VirtualHeapNode(2, 2).level, 1)
        self.assertEqual(VirtualHeapNode(2, 3).level, 2)
        self.assertEqual(VirtualHeapNode(2, 4).level, 2)
        self.assertEqual(VirtualHeapNode(2, 5).level, 2)
        self.assertEqual(VirtualHeapNode(2, 6).level, 2)
        self.assertEqual(VirtualHeapNode(2, 7).level, 3)

        self.assertEqual(VirtualHeapNode(3, 0).level, 0)
        self.assertEqual(VirtualHeapNode(3, 1).level, 1)
        self.assertEqual(VirtualHeapNode(3, 2).level, 1)
        self.assertEqual(VirtualHeapNode(3, 3).level, 1)
        self.assertEqual(VirtualHeapNode(3, 4).level, 2)
        self.assertEqual(VirtualHeapNode(3, 5).level, 2)
        self.assertEqual(VirtualHeapNode(3, 6).level, 2)
        self.assertEqual(VirtualHeapNode(3, 7).level, 2)
        self.assertEqual(VirtualHeapNode(3, 8).level, 2)
        self.assertEqual(VirtualHeapNode(3, 9).level, 2)
        self.assertEqual(VirtualHeapNode(3, 10).level, 2)
        self.assertEqual(VirtualHeapNode(3, 11).level, 2)
        self.assertEqual(VirtualHeapNode(3, 12).level, 2)
        self.assertEqual(VirtualHeapNode(3, 13).level, 3)

    def test_hash(self):
        x1 = VirtualHeapNode(3, 5)
        x2 = VirtualHeapNode(2, 5)
        self.assertNotEqual(id(x1), id(x2))
        self.assertEqual(x1, x2)
        self.assertEqual(x1, x1)
        self.assertEqual(x2, x2)

        all_node_set = set()
        all_node_list = list()
        for k in _test_bases:
            node_set = set()
            node_list = list()
            for height in range(k+2):
                node = VirtualHeapNode(k, height)
                node_set.add(node)
                all_node_set.add(node)
                node_list.append(node)
                all_node_list.append(node)
            self.assertEqual(sorted(node_set),
                             sorted(node_list))
        self.assertNotEqual(sorted(all_node_set),
                            sorted(all_node_list))
    def test_lt(self):
        self.assertEqual(VirtualHeapNode(3, 5) < 4, False)
        self.assertEqual(VirtualHeapNode(3, 5) < 5, False)
        self.assertEqual(VirtualHeapNode(3, 5) < 6, True)

    def test_le(self):
        self.assertEqual(VirtualHeapNode(3, 5) <= 4, False)
        self.assertEqual(VirtualHeapNode(3, 5) <= 5, True)
        self.assertEqual(VirtualHeapNode(3, 5) <= 6, True)

    def test_eq(self):
        self.assertEqual(VirtualHeapNode(3, 5) == 4, False)
        self.assertEqual(VirtualHeapNode(3, 5) == 5, True)
        self.assertEqual(VirtualHeapNode(3, 5) == 6, False)

    def test_ne(self):
        self.assertEqual(VirtualHeapNode(3, 5) != 4, True)
        self.assertEqual(VirtualHeapNode(3, 5) != 5, False)
        self.assertEqual(VirtualHeapNode(3, 5) != 6, True)

    def test_gt(self):
        self.assertEqual(VirtualHeapNode(3, 5) > 4, True)
        self.assertEqual(VirtualHeapNode(3, 5) > 5, False)
        self.assertEqual(VirtualHeapNode(3, 5) > 6, False)

    def test_ge(self):
        self.assertEqual(VirtualHeapNode(3, 5) >= 4, True)
        self.assertEqual(VirtualHeapNode(3, 5) >= 5, True)
        self.assertEqual(VirtualHeapNode(3, 5) >= 6, False)

    def test_LastCommonLevel_k2(self):
        n0 = VirtualHeapNode(2, 0)
        n1 = VirtualHeapNode(2, 1)
        n2 = VirtualHeapNode(2, 2)
        n3 = VirtualHeapNode(2, 3)
        n4 = VirtualHeapNode(2, 4)
        n5 = VirtualHeapNode(2, 5)
        n6 = VirtualHeapNode(2, 6)
        n7 = VirtualHeapNode(2, 7)
        self.assertEqual(n0.LastCommonLevel(n0), 0)
        self.assertEqual(n0.LastCommonLevel(n1), 0)
        self.assertEqual(n0.LastCommonLevel(n2), 0)
        self.assertEqual(n0.LastCommonLevel(n3), 0)
        self.assertEqual(n0.LastCommonLevel(n4), 0)
        self.assertEqual(n0.LastCommonLevel(n5), 0)
        self.assertEqual(n0.LastCommonLevel(n6), 0)
        self.assertEqual(n0.LastCommonLevel(n7), 0)

        self.assertEqual(n1.LastCommonLevel(n0), 0)
        self.assertEqual(n1.LastCommonLevel(n1), 1)
        self.assertEqual(n1.LastCommonLevel(n2), 0)
        self.assertEqual(n1.LastCommonLevel(n3), 1)
        self.assertEqual(n1.LastCommonLevel(n4), 1)
        self.assertEqual(n1.LastCommonLevel(n5), 0)
        self.assertEqual(n1.LastCommonLevel(n6), 0)
        self.assertEqual(n1.LastCommonLevel(n7), 1)

        self.assertEqual(n2.LastCommonLevel(n0), 0)
        self.assertEqual(n2.LastCommonLevel(n1), 0)
        self.assertEqual(n2.LastCommonLevel(n2), 1)
        self.assertEqual(n2.LastCommonLevel(n3), 0)
        self.assertEqual(n2.LastCommonLevel(n4), 0)
        self.assertEqual(n2.LastCommonLevel(n5), 1)
        self.assertEqual(n2.LastCommonLevel(n6), 1)
        self.assertEqual(n2.LastCommonLevel(n7), 0)

        self.assertEqual(n3.LastCommonLevel(n0), 0)
        self.assertEqual(n3.LastCommonLevel(n1), 1)
        self.assertEqual(n3.LastCommonLevel(n2), 0)
        self.assertEqual(n3.LastCommonLevel(n3), 2)
        self.assertEqual(n3.LastCommonLevel(n4), 1)
        self.assertEqual(n3.LastCommonLevel(n5), 0)
        self.assertEqual(n3.LastCommonLevel(n6), 0)
        self.assertEqual(n3.LastCommonLevel(n7), 2)

        self.assertEqual(n4.LastCommonLevel(n0), 0)
        self.assertEqual(n4.LastCommonLevel(n1), 1)
        self.assertEqual(n4.LastCommonLevel(n2), 0)
        self.assertEqual(n4.LastCommonLevel(n3), 1)
        self.assertEqual(n4.LastCommonLevel(n4), 2)
        self.assertEqual(n4.LastCommonLevel(n5), 0)
        self.assertEqual(n4.LastCommonLevel(n6), 0)
        self.assertEqual(n4.LastCommonLevel(n7), 1)

        self.assertEqual(n5.LastCommonLevel(n0), 0)
        self.assertEqual(n5.LastCommonLevel(n1), 0)
        self.assertEqual(n5.LastCommonLevel(n2), 1)
        self.assertEqual(n5.LastCommonLevel(n3), 0)
        self.assertEqual(n5.LastCommonLevel(n4), 0)
        self.assertEqual(n5.LastCommonLevel(n5), 2)
        self.assertEqual(n5.LastCommonLevel(n6), 1)
        self.assertEqual(n5.LastCommonLevel(n7), 0)

        self.assertEqual(n6.LastCommonLevel(n0), 0)
        self.assertEqual(n6.LastCommonLevel(n1), 0)
        self.assertEqual(n6.LastCommonLevel(n2), 1)
        self.assertEqual(n6.LastCommonLevel(n3), 0)
        self.assertEqual(n6.LastCommonLevel(n4), 0)
        self.assertEqual(n6.LastCommonLevel(n5), 1)
        self.assertEqual(n6.LastCommonLevel(n6), 2)
        self.assertEqual(n6.LastCommonLevel(n7), 0)

        self.assertEqual(n7.LastCommonLevel(n0), 0)
        self.assertEqual(n7.LastCommonLevel(n1), 1)
        self.assertEqual(n7.LastCommonLevel(n2), 0)
        self.assertEqual(n7.LastCommonLevel(n3), 2)
        self.assertEqual(n7.LastCommonLevel(n4), 1)
        self.assertEqual(n7.LastCommonLevel(n5), 0)
        self.assertEqual(n7.LastCommonLevel(n6), 0)
        self.assertEqual(n7.LastCommonLevel(n7), 3)

    def test_LastCommonLevel_k3(self):
        n0 = VirtualHeapNode(3, 0)
        n1 = VirtualHeapNode(3, 1)
        n2 = VirtualHeapNode(3, 2)
        n3 = VirtualHeapNode(3, 3)
        n4 = VirtualHeapNode(3, 4)
        n5 = VirtualHeapNode(3, 5)
        n6 = VirtualHeapNode(3, 6)
        n7 = VirtualHeapNode(3, 7)
        self.assertEqual(n0.LastCommonLevel(n0), 0)
        self.assertEqual(n0.LastCommonLevel(n1), 0)
        self.assertEqual(n0.LastCommonLevel(n2), 0)
        self.assertEqual(n0.LastCommonLevel(n3), 0)
        self.assertEqual(n0.LastCommonLevel(n4), 0)
        self.assertEqual(n0.LastCommonLevel(n5), 0)
        self.assertEqual(n0.LastCommonLevel(n6), 0)
        self.assertEqual(n0.LastCommonLevel(n7), 0)

        self.assertEqual(n1.LastCommonLevel(n0), 0)
        self.assertEqual(n1.LastCommonLevel(n1), 1)
        self.assertEqual(n1.LastCommonLevel(n2), 0)
        self.assertEqual(n1.LastCommonLevel(n3), 0)
        self.assertEqual(n1.LastCommonLevel(n4), 1)
        self.assertEqual(n1.LastCommonLevel(n5), 1)
        self.assertEqual(n1.LastCommonLevel(n6), 1)
        self.assertEqual(n1.LastCommonLevel(n7), 0)

        self.assertEqual(n2.LastCommonLevel(n0), 0)
        self.assertEqual(n2.LastCommonLevel(n1), 0)
        self.assertEqual(n2.LastCommonLevel(n2), 1)
        self.assertEqual(n2.LastCommonLevel(n3), 0)
        self.assertEqual(n2.LastCommonLevel(n4), 0)
        self.assertEqual(n2.LastCommonLevel(n5), 0)
        self.assertEqual(n2.LastCommonLevel(n6), 0)
        self.assertEqual(n2.LastCommonLevel(n7), 1)

        self.assertEqual(n3.LastCommonLevel(n0), 0)
        self.assertEqual(n3.LastCommonLevel(n1), 0)
        self.assertEqual(n3.LastCommonLevel(n2), 0)
        self.assertEqual(n3.LastCommonLevel(n3), 1)
        self.assertEqual(n3.LastCommonLevel(n4), 0)
        self.assertEqual(n3.LastCommonLevel(n5), 0)
        self.assertEqual(n3.LastCommonLevel(n6), 0)
        self.assertEqual(n3.LastCommonLevel(n7), 0)

        self.assertEqual(n4.LastCommonLevel(n0), 0)
        self.assertEqual(n4.LastCommonLevel(n1), 1)
        self.assertEqual(n4.LastCommonLevel(n2), 0)
        self.assertEqual(n4.LastCommonLevel(n3), 0)
        self.assertEqual(n4.LastCommonLevel(n4), 2)
        self.assertEqual(n4.LastCommonLevel(n5), 1)
        self.assertEqual(n4.LastCommonLevel(n6), 1)
        self.assertEqual(n4.LastCommonLevel(n7), 0)

        self.assertEqual(n5.LastCommonLevel(n0), 0)
        self.assertEqual(n5.LastCommonLevel(n1), 1)
        self.assertEqual(n5.LastCommonLevel(n2), 0)
        self.assertEqual(n5.LastCommonLevel(n3), 0)
        self.assertEqual(n5.LastCommonLevel(n4), 1)
        self.assertEqual(n5.LastCommonLevel(n5), 2)
        self.assertEqual(n5.LastCommonLevel(n6), 1)
        self.assertEqual(n5.LastCommonLevel(n7), 0)

        self.assertEqual(n6.LastCommonLevel(n0), 0)
        self.assertEqual(n6.LastCommonLevel(n1), 1)
        self.assertEqual(n6.LastCommonLevel(n2), 0)
        self.assertEqual(n6.LastCommonLevel(n3), 0)
        self.assertEqual(n6.LastCommonLevel(n4), 1)
        self.assertEqual(n6.LastCommonLevel(n5), 1)
        self.assertEqual(n6.LastCommonLevel(n6), 2)
        self.assertEqual(n6.LastCommonLevel(n7), 0)

        self.assertEqual(n7.LastCommonLevel(n0), 0)
        self.assertEqual(n7.LastCommonLevel(n1), 0)
        self.assertEqual(n7.LastCommonLevel(n2), 1)
        self.assertEqual(n7.LastCommonLevel(n3), 0)
        self.assertEqual(n7.LastCommonLevel(n4), 0)
        self.assertEqual(n7.LastCommonLevel(n5), 0)
        self.assertEqual(n7.LastCommonLevel(n6), 0)
        self.assertEqual(n7.LastCommonLevel(n7), 2)

    def test_ChildNode(self):
        root = VirtualHeapNode(2, 0)
        self.assertEqual(list(_do_preorder(root)),
                         [0, 1, 3, 4, 2, 5, 6])
        self.assertEqual(list(_do_postorder(root)),
                         [3, 4, 1, 5, 6, 2, 0])
        self.assertEqual(list(_do_inorder(root)),
                         [3, 1, 4, 0, 5, 2, 6])

        root = VirtualHeapNode(3, 0)
        self.assertEqual(
            list(_do_preorder(root)),
            [0, 1, 4, 5, 6, 2, 7, 8, 9, 3, 10, 11, 12])
        self.assertEqual(
            list(_do_postorder(root)),
            [4, 5, 6, 1, 7, 8, 9, 2, 10, 11, 12, 3, 0])

    def test_ParentNode(self):
        self.assertEqual(VirtualHeapNode(2, 0).ParentNode(),
                         None)
        self.assertEqual(VirtualHeapNode(2, 1).ParentNode(),
                         VirtualHeapNode(2, 0))
        self.assertEqual(VirtualHeapNode(2, 2).ParentNode(),
                         VirtualHeapNode(2, 0))
        self.assertEqual(VirtualHeapNode(2, 3).ParentNode(),
                         VirtualHeapNode(2, 1))
        self.assertEqual(VirtualHeapNode(2, 4).ParentNode(),
                         VirtualHeapNode(2, 1))
        self.assertEqual(VirtualHeapNode(2, 5).ParentNode(),
                         VirtualHeapNode(2, 2))
        self.assertEqual(VirtualHeapNode(2, 6).ParentNode(),
                         VirtualHeapNode(2, 2))
        self.assertEqual(VirtualHeapNode(2, 7).ParentNode(),
                         VirtualHeapNode(2, 3))

        self.assertEqual(VirtualHeapNode(3, 0).ParentNode(),
                         None)
        self.assertEqual(VirtualHeapNode(3, 1).ParentNode(),
                         VirtualHeapNode(3, 0))
        self.assertEqual(VirtualHeapNode(3, 2).ParentNode(),
                         VirtualHeapNode(3, 0))
        self.assertEqual(VirtualHeapNode(3, 3).ParentNode(),
                         VirtualHeapNode(3, 0))
        self.assertEqual(VirtualHeapNode(3, 4).ParentNode(),
                         VirtualHeapNode(3, 1))
        self.assertEqual(VirtualHeapNode(3, 5).ParentNode(),
                         VirtualHeapNode(3, 1))
        self.assertEqual(VirtualHeapNode(3, 6).ParentNode(),
                         VirtualHeapNode(3, 1))
        self.assertEqual(VirtualHeapNode(3, 7).ParentNode(),
                         VirtualHeapNode(3, 2))
        self.assertEqual(VirtualHeapNode(3, 8).ParentNode(),
                         VirtualHeapNode(3, 2))
        self.assertEqual(VirtualHeapNode(3, 9).ParentNode(),
                         VirtualHeapNode(3, 2))
        self.assertEqual(VirtualHeapNode(3, 10).ParentNode(),
                         VirtualHeapNode(3, 3))
        self.assertEqual(VirtualHeapNode(3, 11).ParentNode(),
                         VirtualHeapNode(3, 3))
        self.assertEqual(VirtualHeapNode(3, 12).ParentNode(),
                         VirtualHeapNode(3, 3))
        self.assertEqual(VirtualHeapNode(3, 13).ParentNode(),
                         VirtualHeapNode(3, 4))

    def test_AncestorNodeAtLevel(self):
        self.assertEqual(VirtualHeapNode(2, 0).AncestorNodeAtLevel(0),
                         VirtualHeapNode(2, 0))
        self.assertEqual(VirtualHeapNode(2, 0).AncestorNodeAtLevel(1),
                         None)
        self.assertEqual(VirtualHeapNode(2, 1).AncestorNodeAtLevel(0),
                         VirtualHeapNode(2, 0))
        self.assertEqual(VirtualHeapNode(2, 1).AncestorNodeAtLevel(1),
                         VirtualHeapNode(2, 1))
        self.assertEqual(VirtualHeapNode(2, 1).AncestorNodeAtLevel(2),
                         None)
        self.assertEqual(VirtualHeapNode(2, 3).AncestorNodeAtLevel(0),
                         VirtualHeapNode(2, 0))
        self.assertEqual(VirtualHeapNode(2, 3).AncestorNodeAtLevel(1),
                         VirtualHeapNode(2, 1))
        self.assertEqual(VirtualHeapNode(2, 3).AncestorNodeAtLevel(2),
                         VirtualHeapNode(2, 3))
        self.assertEqual(VirtualHeapNode(2, 3).AncestorNodeAtLevel(3),
                         None)

        self.assertEqual(VirtualHeapNode(3, 0).AncestorNodeAtLevel(0),
                         VirtualHeapNode(3, 0))
        self.assertEqual(VirtualHeapNode(3, 0).AncestorNodeAtLevel(1),
                         None)
        self.assertEqual(VirtualHeapNode(3, 1).AncestorNodeAtLevel(0),
                         VirtualHeapNode(3, 0))
        self.assertEqual(VirtualHeapNode(3, 1).AncestorNodeAtLevel(1),
                         VirtualHeapNode(3, 1))
        self.assertEqual(VirtualHeapNode(3, 1).AncestorNodeAtLevel(2),
                         None)
        self.assertEqual(VirtualHeapNode(3, 4).AncestorNodeAtLevel(0),
                         VirtualHeapNode(3, 0))
        self.assertEqual(VirtualHeapNode(3, 4).AncestorNodeAtLevel(1),
                         VirtualHeapNode(3, 1))
        self.assertEqual(VirtualHeapNode(3, 4).AncestorNodeAtLevel(2),
                         VirtualHeapNode(3, 4))
        self.assertEqual(VirtualHeapNode(3, 4).AncestorNodeAtLevel(3),
                         None)

    def test_GenerateBucketPathToRoot(self):
        self.assertEqual(list(VirtualHeapNode(2, 0).GenerateBucketPathToRoot()),
                         list(reversed([0])))
        self.assertEqual(list(VirtualHeapNode(2, 7).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 3, 7])))
        self.assertEqual(list(VirtualHeapNode(2, 8).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 3, 8])))
        self.assertEqual(list(VirtualHeapNode(2, 9).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 4, 9])))
        self.assertEqual(list(VirtualHeapNode(2, 10).GenerateBucketPathToRoot()),
                         list(reversed([0, 1, 4, 10])))
        self.assertEqual(list(VirtualHeapNode(2, 11).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 5, 11])))
        self.assertEqual(list(VirtualHeapNode(2, 12).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 5, 12])))
        self.assertEqual(list(VirtualHeapNode(2, 13).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 6, 13])))
        self.assertEqual(list(VirtualHeapNode(2, 14).GenerateBucketPathToRoot()),
                         list(reversed([0, 2, 6, 14])))

    def test_BucketPath(self):
        self.assertEqual(VirtualHeapNode(2, 0).BucketPath(),
                         [0])
        self.assertEqual(VirtualHeapNode(2, 7).BucketPath(),
                         [0, 1, 3, 7])
        self.assertEqual(VirtualHeapNode(2, 8).BucketPath(),
                         [0, 1, 3, 8])
        self.assertEqual(VirtualHeapNode(2, 9).BucketPath(),
                         [0, 1, 4, 9])
        self.assertEqual(VirtualHeapNode(2, 10).BucketPath(),
                         [0, 1, 4, 10])
        self.assertEqual(VirtualHeapNode(2, 11).BucketPath(),
                         [0, 2, 5, 11])
        self.assertEqual(VirtualHeapNode(2, 12).BucketPath(),
                         [0, 2, 5, 12])
        self.assertEqual(VirtualHeapNode(2, 13).BucketPath(),
                         [0, 2, 6, 13])
        self.assertEqual(VirtualHeapNode(2, 14).BucketPath(),
                         [0, 2, 6, 14])

    def test_repr(self):
        self.assertEqual(
            repr(VirtualHeapNode(2, 0)),
            "VirtualHeapNode(k=2, bucket=0, level=0, label='')")
        self.assertEqual(
            repr(VirtualHeapNode(2, 7)),
            "VirtualHeapNode(k=2, bucket=7, level=3, label='000')")
        self.assertEqual(
            repr(VirtualHeapNode(3, 0)),
            "VirtualHeapNode(k=3, bucket=0, level=0, label='')")
        self.assertEqual(
            repr(VirtualHeapNode(3, 7)),
            "VirtualHeapNode(k=3, bucket=7, level=2, label='10')")
        self.assertEqual(
            repr(VirtualHeapNode(5, 25)),
            "VirtualHeapNode(k=5, bucket=25, level=2, label='34')")

    def test_str(self):
        self.assertEqual(
            str(VirtualHeapNode(2, 0)),
            "(0, 0)")
        self.assertEqual(
            str(VirtualHeapNode(2, 7)),
            "(3, 0)")
        self.assertEqual(
            str(VirtualHeapNode(3, 0)),
            "(0, 0)")
        self.assertEqual(
            str(VirtualHeapNode(3, 7)),
            "(2, 3)")
        self.assertEqual(
            str(VirtualHeapNode(5, 25)),
            "(2, 19)")

    def test_Label(self):

        self.assertEqual(VirtualHeapNode(2, 0).Label(), "")
        self.assertEqual(VirtualHeapNode(2, 1).Label(), "0")
        self.assertEqual(VirtualHeapNode(2, 2).Label(), "1")
        self.assertEqual(VirtualHeapNode(2, 3).Label(), "00")
        self.assertEqual(VirtualHeapNode(2, 4).Label(), "01")
        self.assertEqual(VirtualHeapNode(2, 5).Label(), "10")
        self.assertEqual(VirtualHeapNode(2, 6).Label(), "11")
        self.assertEqual(VirtualHeapNode(2, 7).Label(), "000")
        self.assertEqual(VirtualHeapNode(2, 8).Label(), "001")
        self.assertEqual(VirtualHeapNode(2, 9).Label(), "010")
        self.assertEqual(VirtualHeapNode(2, 10).Label(), "011")
        self.assertEqual(VirtualHeapNode(2, 11).Label(), "100")
        self.assertEqual(VirtualHeapNode(2, 12).Label(), "101")
        self.assertEqual(VirtualHeapNode(2, 13).Label(), "110")
        self.assertEqual(VirtualHeapNode(2, 14).Label(), "111")
        self.assertEqual(VirtualHeapNode(2, 15).Label(), "0000")
        self.assertEqual(VirtualHeapNode(2, 30).Label(), "1111")

        for k in _test_labeled_bases:
            for b in range(VirtualHeap.\
                           CalculateBucketCountInHeapWithLevels(k, 3)+1):
                label = VirtualHeapNode(k, b).Label()
                level = VirtualHeapNode(k, b).level
                if label == "":
                    self.assertEqual(b, 0)
                else:
                    self.assertEqual(
                        b,
                        VirtualHeap.BaseKStringToBase10Integer(k, label) + \
                        VirtualHeap.CalculateBucketCountInHeapWithLevels(k, level))

    def test_IsNodeOnPath(self):
        self.assertEqual(
            VirtualHeapNode(2, 0).IsNodeOnPath(
                VirtualHeapNode(2, 0)),
            True)
        self.assertEqual(
            VirtualHeapNode(2, 0).IsNodeOnPath(
                VirtualHeapNode(2, 1)),
            False)
        self.assertEqual(
            VirtualHeapNode(2, 0).IsNodeOnPath(
                VirtualHeapNode(2, 2)),
            False)
        self.assertEqual(
            VirtualHeapNode(2, 0).IsNodeOnPath(
                VirtualHeapNode(2, 3)),
            False)

        self.assertEqual(
            VirtualHeapNode(5, 20).IsNodeOnPath(
                VirtualHeapNode(5, 21)),
            False)
        self.assertEqual(
            VirtualHeapNode(5, 21).IsNodeOnPath(
                VirtualHeapNode(5, 4)),
            True)


        self.assertEqual(
            VirtualHeapNode(3, 7).IsNodeOnPath(
                VirtualHeapNode(3, 0)),
            True)
        self.assertEqual(
            VirtualHeapNode(3, 7).IsNodeOnPath(
                VirtualHeapNode(3, 2)),
            True)
        self.assertEqual(
            VirtualHeapNode(3, 7).IsNodeOnPath(
                VirtualHeapNode(3, 7)),
            True)
        self.assertEqual(
            VirtualHeapNode(3, 7).IsNodeOnPath(
                VirtualHeapNode(3, 8)),
            False)

class TestVirtualHeap(unittest.TestCase):

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
        for k in _test_bases:
            for height in range(k+2):
                for bucket_size in range(1, 5):
                    vh = VirtualHeap(k, height, bucket_size=bucket_size)
                    cnt = (((k**(height+1))-1)//(k-1))
                    self.assertEqual(vh.BucketCount(), cnt)
                    self.assertEqual(vh.NodeCount(), cnt)
                    self.assertEqual(vh.SlotCount(), cnt * bucket_size)

    def test_ObjectCountAtLevel(self):
        for k in _test_bases:
            for height in range(k+2):
                for bucket_size in range(1, 5):
                    vh = VirtualHeap(k, height, bucket_size=bucket_size)
                    for l in range(height+1):
                        cnt = k**l
                        self.assertEqual(vh.BucketCountAtLevel(l), cnt)
                        self.assertEqual(vh.NodeCountAtLevel(l), cnt)
                        self.assertEqual(vh.SlotCountAtLevel(l),
                                         cnt * bucket_size)

    def test_LeafObjectCount(self):
        for k in _test_bases:
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
