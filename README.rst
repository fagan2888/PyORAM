PyORAM
======

.. image:: https://travis-ci.org/ghackebeil/PyORAM.svg?branch=master
    :target: https://travis-ci.org/ghackebeil/PyORAM

.. image:: https://ci.appveyor.com/api/projects/status/898bxsvqdch1btv6/branch/master?svg=true
    :target: https://ci.appveyor.com/project/ghackebeil/PyORAM?branch=master

.. image:: https://codecov.io/github/ghackebeil/PyORAM/coverage.svg?branch=master
    :target: https://codecov.io/github/ghackebeil/PyORAM?branch=master

Python-based Oblivious RAM (PyORAM) is a collection of
Oblivious RAM algorithms implemented in Python. This package
serves to enable rapid prototyping and testing of new ORAM
algorithms and ORAM-based applications tailored for the
cloud-storage setting. PyORAM is written to support as many
Python versions as possible, including Python 2.7+, Python
3.3+, and PyPy 2.6+.

This software is copyright (c) by Gabriel A. Hackebeil (gabe.hackebeil@gmail.com).

This software is released under the MIT software license.
This license, including disclaimer, is available in the 'LICENSE' file.

Installation
~~~~~~~~~~~~

To install PyORAM, first clone the repository::

  $ git clone https://github.com/ghackebeil/PyORAM.git

Next, enter the directory where PyORAM has been cloned and run setup::

  $ python setup.py install

If you are a developer, you should instead install using::

  $ pip install -e .
  $ pip install nose2 unittest2

Installation Tips
~~~~~~~~~~~~~~~~~

* If you have trouble installing the cryptography package
  on OS X with PyPy: `stackoverflow <https://stackoverflow.com/questions/36662704/fatal-error-openssl-e-os2-h-file-not-found-in-pypy/36706513#36706513>`_.
* If you encounter the dreaded "unable to find
  vcvarsall.bat" error when installing packages with C
  extensions through pip in Windows: `blog post <https://blogs.msdn.microsoft.com/pythonengineering/2016/04/11/unable-to-find-vcvarsall-bat>`_.

Algorithms Available (So Far)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* `Path ORAM <http://arxiv.org/abs/1202.5150v3>`_

 - Generalized to work over k-kary storage heaps in order
   to study stash-size behavior in this setting. Default
   settings will use a binary storage heap.

 - Includes interfaces for local storage, SFTP storage
   (using paramiko), and Amazon S3 storage (using
   boto3). See examples:

  + examples/path_oram_mmap.py

  + examples/path_oram_file.py

  + examples/path_oram_sftp.py

  + examples/path_oram_s3.py

Why Python?
~~~~~~~~~~~

This project is meant for research. It is provided mainly as
a tool for other researchers studying the applicability of
ORAM to the cloud-storage setting. In such a setting, we
observe that network latency far outweighs any overhead
introduced from switching to an interpreted language such as
Python (as opposed to C++ or Java). Thus, our hope is that
by providing a Python-based library of ORAM tools, we will
enable researchers to spend more time prototyping new and
interesting ORAM applications and less time fighting with a
compiler or chasing down segmentation faults.
