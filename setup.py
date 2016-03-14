# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
def _readme():
    with open(path.join(here, 'README.md'), encoding='utf-8') as f:
        return f.read()

import pyoram

setup(
    name='pyoram',
    version=pyoram.__version__,
    description='PyORAM: Python-based Oblivious RAM',
    long_description=_readme(),
    url='https://github.com/ghackebeil/PyORAM',
    author='Gabriel A. Hackebeil',
    author_email='gabe.hackebeil@gmail.com',
    license='MIT',
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
    setup_requires=["cffi>=1.0.0"],
    install_requires=['pycrypto',
                      'cffi>=1.0.0',
                      'boto3',
                      'six'],
    cffi_modules=["pyoram/tree/virtualheap_build.py:ffi"],
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
