#!/usr/bin/env python

from pprint import pprint
import os
import sys
import time
sys.stderr.write(os.getcwd())
sys.stderr.flush()

logfile = open(os.path.join(os.path.dirname(__file__), 'mw.log'), 'a+')

def log(data):
    pprint(line, logfile)
    logfile.flush()

try:
    now = time.time()
    while True and time.time() < now + 5:
        line = sys.stdin.readline().strip()
        log(line)
        if not line or 'shutdown' in line:
            break
        time.sleep(1)
except IOError:
    pass
