#!/bin/sh
set -x
exec exabgp -e $(pwd)/env.ini mw.conf "$@"
