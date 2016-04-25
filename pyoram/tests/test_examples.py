import os
import glob
import sys
import unittest

thisfile = os.path.abspath(__file__)
thisdir = os.path.dirname(thisfile)
topdir = os.path.dirname(
    os.path.dirname(thisdir))
exdir = os.path.join(topdir, 'examples')
examples = glob.glob(os.path.join(exdir,"*.py"))

assert os.path.exists(exdir)
assert thisfile not in examples

tdict = {}
for fname in examples:
    basename = os.path.basename(fname)
    assert basename.endswith('.py')
    assert len(basename) >= 3
    basename = basename[:-3]
    tname = 'test_'+basename
    tdict[tname] = fname, basename

assert len(tdict) == len(examples)

def _execute_example(example_name):
    filename, basename = tdict[example_name]
    assert os.path.exists(filename)
    try:
        sys.path.insert(0, exdir)
        m = __import__(basename)
        m.main()
    finally:
        sys.path.remove(exdir)

# this is recognized by nosetests as
# a dynamic test generator
def test_generator():
    for example_name in sorted(tdict):
        yield _execute_example, example_name

if __name__ == "__main__":
    for tfunc, tname in test_generator():              # pragma: no cover
        tfunc(tname)                                   # pragma: no cover
