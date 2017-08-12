#!/bin/bash
set -eufo pipefail

pmbgpd -l 1179 -f pmbgpd.conf | lua main.lua
