from pyoram.__about__ import __version__

def _configure_logging():
    import os
    import logging


    log = logging.getLogger("pyoram")
    formatter = logging.Formatter(
        fmt=("[%(asctime)s.%(msecs)03d,"
             "%(name)s,%(levelname)s] %(threadName)s %(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S")

    level = os.environ.get("PYORAM_LOGLEVEL")
    logfilename = os.environ.get("PYORAM_LOGFILE", "pyoram.log")
    if logfilename == "{stderr}":
        logfilename = None

    if level not in (None, ""):
        levelvalue = getattr(logging, level)
        if len(logging.root.handlers) == 0:
            # configure the logging with some sensible
            # defaults.
            try:
                import tempfile
                tempfile = tempfile.TemporaryFile(dir=".")
                tempfile.close()
            except OSError:
                # cannot write in current directory, use the
                # console logger
                handler = logging.StreamHandler()
            else:
                if logfilename is None:
                    handler = logging.StreamHandler()
                else:
                    # set up a basic logfile in current directory
                    handler = logging.FileHandler(logfilename)
            handler.setFormatter(formatter)
            handler.setLevel(level)
            log.addHandler(handler)
            log.setLevel(level)
            log.info("PyORAM log configured using built-in "
                     "defaults, level=%s", level)
    else:
        # logging is disabled
        log.setLevel(9999)

_configure_logging()
del _configure_logging

import pyoram.util
import pyoram.crypto
import pyoram.storage
import pyoram.encrypted_storage
import pyoram.oblivious_storage
