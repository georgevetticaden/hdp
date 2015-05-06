#!/bin/sh
export PATH="$(dirname $(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)):$PATH"
"$@"
