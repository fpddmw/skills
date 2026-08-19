#!/bin/sh

set -eu

export LD_LIBRARY_PATH="/opt/broken-poppler/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
exec /usr/bin/pdftoppm "$@"
