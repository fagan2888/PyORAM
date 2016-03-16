import os
import glob
import sys
import unittest

thisfile = os.path.abspath(__file__)
thisdir = os.path.dirname(thisfile)
exdir = os.path.dirname(thisdir)
examples = glob.glob(os.path.join(exdir,"*.py"))

assert thisfile not in examples

tdict = {}
for fname in examples:
    basename = os.path.basename(fname)
    assert basename.endswith('.py')
    assert len(basename) >= 3
    basename = basename[:-3]
    tname = 'test_'+basename
    tdict[tname] = fname

assert len(tdict) == len(examples)

def _execute_example(example_name):
    filename = tdict[example_name]
    assert os.path.exists(filename)
    rc = os.system(sys.executable+' '+filename)
    if rc:
        raise AssertionError(                          # pragma: no cover
            "Bad return code for example '%s': %s"
            % (example_name, rc))

# this is recognized by nosetests as
# a dynamic test generator
def test_generator():
    for example_name in sorted(tdict):
        yield _execute_example, example_name

if __name__ == "__main__":
    for tfunc, tname in test_generator():              # pragma: no cover
        tfunc(tname)                                   # pragma: no cover
