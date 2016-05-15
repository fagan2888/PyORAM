#!/bin/bash
set -e

pip install jip
jip install $JYTHON
NON_GROUP_ID=${JYTHON#*:}
_JYTHON_BASENAME=${NON_GROUP_ID/:/-}
OLD_VIRTUAL_ENV=${VIRTUAL_ENV:=.}
java -jar $OLD_VIRTUAL_ENV/javalib/${_JYTHON_BASENAME}.jar -s -d $HOME/jython
$HOME/jython/bin/jython -c "import sys; print(sys.version_info)"
virtualenv --version
virtualenv -p $HOME/jython/bin/jython $HOME/myvirtualenv
