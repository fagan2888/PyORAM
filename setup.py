import os
import sys
import platform
from setuptools import setup
from codecs import open

here = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join("pyoram", "__about__.py")) as f:
    exec(f.read(), about)

# Get the long description from the README file
def _readme():
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        return f.read()

setup_requirements = []
requirements = ['boto3',
                'six']
#                'pycrypto']

if platform.python_implementation() == "PyPy":
    if sys.pypy_version_info < (2, 6):
        raise RuntimeError(
            "PyORAM is not compatible with PyPy < 2.6. Please "
            "upgrade PyPy to use this library.")
else:
    if sys.version_info <= (2, 6):
        raise RuntimeError(
            "PyORAM is not compatible with Python < 2.7. Please "
            "upgrade Python to use this library.")
    requirements.append("cffi>=1.0.0")
    setup_requirements.append("cffi>=1.0.0")

setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__summary__'],
    long_description=_readme(),
    url=about['__uri__'],
    author=about['__author__'],
    author_email=about['__email__'],
    license=about['__license__'],
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 2 - Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Security :: Cryptography',
        "Natural Language :: English",
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    keywords='oram privacy cryptography',
    packages=['pyoram'],
    setup_requires=setup_requirements,
    install_requires=requirements,
    cffi_modules=["pyoram/storage/virtualheap_build.py:ffi"],
    # use MANIFEST.in
    include_package_data=True,
    #entry_points={
    #    'console_scripts': [
    #        'sample=sample:main',
    #    ],
    #},
    test_suite='nose.collector',
    tests_require=['nose']
)
