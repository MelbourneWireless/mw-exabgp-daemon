#!/usr/bin/env python
from __future__ import print_function, unicode_literals

# stdlib
import datetime
import fileinput
import json
import logging
import os

# requirements
import daiquiri
import psycopg2


daiquiri.setup(level=logging.DEBUG)

DSN = 'postgres:///'
PID = os.getpid()
LOGGER = daiquiri.getLogger()
ROUTES = {}

def log(data):
    LOGGER.debug(
        '{ts} [{pid}] {data!r}\n'.format(
            ts=datetime.datetime.now().isoformat(),
            pid=PID,
            data={str(key): val for key, val in data.items()} if isinstance(data, dict) else data,
        )
    )


print('version')


def execute_sql(sql, *args):
    LOGGER.debug('SQL: %s', sql % args)
    try:
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
    except Exception as err:
        LOGGER.error('%s', sql % args, exc_info=True)


def route_add(peer_asn, subnet, as_path):
    ROUTES.setdefault(subnet, set()).add(peer_asn)
    execute_sql(
        'insert into routes (peer_as, subnet, as_path) values (%s, %s, %s)',
        peer_asn,
        subnet,
        list(as_path),
    )
    LOGGER.debug('route_add(%r, %r, %r)', peer_asn, subnet, as_path)

def route_del(peer_asn, subnet):
    execute_sql(
        'delete from routes where peer_as=%s and subnet=%s',
        peer_asn,
        subnet,
    )
    LOGGER.debug('route_del(%r, %r)', peer_asn, subnet)
    try:
        active_routes = ROUTES[subnet]
    except KeyError:
        return  # no active route matching withdrawal

    if peer_asn not in active_routes:
        return  # peer_asn not associated with an active_route for this subnet

    active_routes.remove(peer_asn)
    if not active_routes:
        del ROUTES[subnet]

def route_del_peer(peer_asn):
    for subnet, peers in list(ROUTES.items()):
        if peer_asn in peers:
            route_del(peer_asn, subnet)

def on_update(address, asn, direction, message, **kwargs):
    """
    Update a route advertisement.

    {
        "exabgp": "4.0.1",
        "time": 1500898535.445492,
        "host": "HP840G3",
        "pid": 13703,
        "ppid": 15954,
        "counter": 70,
        "type": "update",
        "neighbor": {
            "address": {
                "local": "192.168.3.97",
                "peer": "192.168.3.254"
            },
            "asn": {
                "local": 65001,
                "peer": 64570
            },
            "direction": "receive",
            "message": {
                "update": {
                    "attribute": {
                        "origin": "incomplete",
                        "as-path": [
                            64570
                        ],
                        "confederation-path": []
                    },
                    "announce": {
                        "ipv4 unicast": {
                            "192.168.3.254": [
                                {
                                    "nlri": "10.10.132.242/ 32"
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
    """
    del kwargs
    peer_asn = asn['peer']

    if direction != 'receive':
        return  # only interested in received routes

    update = message.get('update', None)
    if update is None:
        return  # not a route update

    on_update_withdraw(peer_asn, address, update)
    on_update_announce(peer_asn, address, update)

    LOGGER.info('{} routes.'.format(len(ROUTES)))


def on_update_announce(peer_asn, address, update):
    if 'attribute' in update and 'announce' in update:
        attribute = update['attribute']
        as_path = tuple(attribute['as-path'])

        for _source, announcements in update.get('announce', {}).items():
            for nexthop, routes in announcements.items():
                for route in routes:
                    for _route_type, subnet in route.items():
                        #subnet = ipaddress.ip_network(subnet)
                        LOGGER.info(
                            'Received route to {subnet} via {as_path} ({nexthop}) from [{peer_asn}] ({peer_address})'.format(
                                peer_asn=peer_asn,
                                peer_address=address['peer'],
                                nexthop=nexthop,
                                subnet=subnet,
                                as_path=as_path,
                            ),
                        )
                        route_add(peer_asn, subnet, as_path)

def on_update_withdraw(peer_asn, address, update):
    for _source, withdrawals in update.get('withdraw', {}).items():
        for withdrawal in withdrawals:
            for _withdrawal_type, subnet in withdrawal.items():
                #subnet = ipaddress.ip_network(subnet)
                LOGGER.info(
                    'Received withdraw from {subnet} via [{peer_asn}] ({peer_address})'.format(
                        peer_asn=peer_asn,
                        peer_address=address['peer'],
                        subnet=subnet,
                    ),
                )
                route_del(peer_asn, subnet)


def on_state(address, asn, state, **kwargs):
    if state == 'down':
        route_del_peer(asn['peer'])


def on_notification(notification):
    if notification == 'shutdown':
        execute_sql('truncate routes')


for line in fileinput.input():
    log(line)
    if '"reason": peer reset,' in line:
        line = line.replace(
            '"reason": peer reset,',
            '"reason": "peer reset,',
        ).replace(
            '] } }\n',
            ']" } }\n',
        )
        LOGGER.warn('Fixed JSON: %r', line)
    try:
        data = json.loads(line)
        msg = data.get('type', None)
        if msg == 'update':
            on_update(**data['neighbor'])
        elif msg == 'state':
            on_state(**data['neighbor'])
        elif msg == 'notification':
            on_notification(data['notification'])
            if data.get('notification') == 'shutdown':
                break
        else:
            log(data)
    except Exception as err:
        LOGGER.error('%s', err, exc_info=True)
