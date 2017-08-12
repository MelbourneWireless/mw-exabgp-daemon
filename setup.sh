#!/bin/bash
set -eufo pipefail

psql -f drop.psql && psql --set BGPD_PASSWORD=${BGPD_PASSWORD} --set AUTHENTICATOR_PASSWORD=${AUTHENTICATOR_PASSWORD} -f setup.psql
