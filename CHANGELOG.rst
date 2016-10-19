Changelog
=========

0.3.0 - `master`_
~~~~~~~~~~~~~~~~~

0.2.0 - 2016-10-18
~~~~~~~~~~~~~~~~~~

* using chunking to speed up yield_blocks for SFTP
* speed up clearing entries in S3 interface by chunking delete requests
* adding helper property to access heap storage on path oram
* use a mmap to store the top-cached heap buckets
* replace the show_status_bar keywords by a global config item
* express status bar units as a memory transfer rate during setup
* tweaks to Path ORAM to make it easier to generalize to other schemes
* changing suffix of S3 index file from txt to bin
* updates to readme

0.1.2 - 2016-05-15
~~~~~~~~~~~~~~~~~~

* Initial release.

.. _`master`: https://github.com/ghackebeil/PyORAM
