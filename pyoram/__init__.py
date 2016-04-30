from pyoram.__about__ import __version__

def _configure_logging():
    import os
    import logging

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
                # default console logger
                logging.basicConfig(level=levelvalue)
            else:
                # set up a basic logfile in current directory
                logging.basicConfig(
                    level=levelvalue,
                    filename=logfilename,
                    datefmt="%Y-%m-%d %H:%M:%S",
                    format=("[%(asctime)s.%(msecs)03d,"
                            "%(name)s,%(levelname)s] %(message)s")
                )
            log = logging.getLogger("PyORAM")
            log.info("PyORAM log configured using built-in "
                     "defaults, level=%s", level)
    else:
        # logging is disabled
        log = logging.getLogger("PyORAM")
        log.setLevel(9999)

_configure_logging()
del _configure_logging

import pyoram.util
import pyoram.crypto
import pyoram.storage
import pyoram.tree
