#!/bin/sh
set -x
export PGUSER=exabgp
exec exabgp -e $(pwd)/env.ini mw.conf "$@"
