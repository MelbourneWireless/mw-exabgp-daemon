#!/usr/bin/env python
from __future__ import print_function, unicode_literals

# stdlib
import fileinput
import json
import logging
import os
import sys

# requirements
import daiquiri
import psycopg2


# configure logging
daiquiri.setup(
    level=logging.DEBUG,
)
# using stdlib logging object, since daiquiri logger swallows unknown kwargs (eg: exc_info mistyped as log_exc)
LOGGER = logging.getLogger(os.path.basename(__file__))


# DB settings via environment: https://www.postgresql.org/docs/current/static/libpq-envars.html
DB = psycopg2.connect('postgres:///')
DB.autocommit = True

PID = os.getpid()
ROUTES = {}


print('version')


def execute_sql(sql, *args, **kwargs):
    query = None
    try:
        try:
            with DB.cursor() as cur:
                try:
                    cur.execute(sql, args)
                    kwargs.get('log', LOGGER.debug)('SQL: %r', cur.query)
                    return cur
                finally:
                    query = cur.query
        finally:
            for notice in DB.notices:
                LOGGER.debug('SQL: %s', notice)
    except Exception:
        if query is None:
            query = [sql, args]
        LOGGER.error('%r', query, exc_info=True)


def route_add(peer_asn, subnet, as_path):
    ROUTES.setdefault(subnet, set()).add(peer_asn)
    execute_sql(
        'insert into routes (peer_as, subnet, as_path) values (%s, %s, %s)',
        peer_asn,
        subnet,
        as_path,
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
    del kwargs  # unused
    peer_asn = asn['peer']

    if direction != 'receive':
        return  # only interested in received routes

    update = message.get('update', None)
    if update is None:
        return  # not a route update

    on_update_withdraw(peer_asn, address, update)
    on_update_announce(peer_asn, address, update)

    LOGGER.debug('{} routes.'.format(len(ROUTES)))


def on_update_announce(peer_asn, address, update):
    if 'attribute' in update and 'announce' in update:
        attribute = update['attribute']
        as_path = attribute.get('as-path', [])

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
    del kwargs  # unused
    if state == 'down':
        route_del_peer(asn['peer'])
        LOGGER.warn('Peer down (%s), cleared routes.', address['peer'])


def on_notification(notification):
    if notification == 'shutdown':
        execute_sql('truncate routes', log=LOGGER.info)
        return 0  # return code from main


def main(input):
    # read lines from input
    for line in input:
        LOGGER.debug('STDIN: %r', line)

        # fix stupid hand-rolled JSON encoding done by exabgp (it *should* use json.dumps instead)
        fixed = line
        for invalid, replacement in [
            # https://github.com/Exa-Networks/exabgp/issues/686
            ('"reason": peer reset,', '"reason": "peer reset,'),
            ('d] } }\n', 'd]" } }\n'),
            # no ticket logged yet
            ('"peer": None } ,', '"peer": null } ,'),
        ]:
            fixed = fixed.replace(invalid, replacement)
        if line != fixed:
            LOGGER.warn('Corrected JSON: %s', fixed)
            line = fixed

        # parse JSON and dispatch to handler function
        data = json.loads(line)
        msg = data.get('type', None)
        if msg == 'update':
            on_update(**data['neighbor'])
        elif msg == 'state':
            on_state(**data['neighbor'])
        elif msg == 'notification':
            result = on_notification(data['notification'])
            if result is not None:
                return result
        else:
            LOGGER.warn('Unknown type: %r', msg)


if __name__ == '__main__':
    try:
        # fileinput allows us to test by supplying one or more input filenames as program arguments
        sys.exit(main(fileinput.input()))
    except Exception as err:
        LOGGER.error('%s', err, exc_info=True)
        sys.exit(1)
