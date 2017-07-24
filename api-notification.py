#!/usr/bin/env python
from __future__ import print_function, unicode_literals
import datetime
import fileinput
import json
import os
import sys


PID = os.getpid()
LOG = open(os.path.join(os.path.dirname(__file__), 'mw.log'), 'a+')


def log(data):
    LOG.write(
        '{ts} [{pid}] {data!r}\n'.format(
            ts=datetime.datetime.now().isoformat(),
            pid=PID,
            data={str(key): val for key, val in data.items()} if isinstance(data, dict) else data,
        )
    )
    LOG.flush()


print('version')


def on_update(address, asn, direction, message, **kwargs):
    """
    Update a route advertisement.
    """
    if direction != 'receive':
        return  # only interested in received routes

    update = message.get('update', None)
    if update is None:
        return  # not a route update

    attribute = update['attribute']
    as_path = attribute['as-path']

    for source, announcements in update.get('announce', {}).items():
        for nexthop, routes in announcements.items():
            for route in routes:
                for route_type, subnet in route.items():
                    network, prefix = subnet.split('/')
                    if not network.startswith('10.10.'):
                        continue  # not a MW route
                    if not prefix.isdigit():
                        continue  # invalid prefix
                    prefix = int(prefix)
                    if not 24 <= prefix <= 32:
                        continue  # invalid prefix length
                    print(
                        'Received route to {subnet} via {as_path} ({nexthop}) from [{peer_asn}] ({peer_address})'.format(
                            peer_asn=asn['peer'],
                            peer_address=address['peer'],
                            nexthop=nexthop,
                            subnet=subnet,
                            as_path=as_path,
                        ),
                        file=sys.stderr,
                    )


for line in fileinput.input():
    log(line)
    try:
        data = json.loads(line)
        msg = data.get('type', None)
        if msg == 'update':
            on_update(**data['neighbor'])
        else:
            log(data)
            if data.get('notification') == 'shutdown':
                break
    except Exception as err:
        log(err)
