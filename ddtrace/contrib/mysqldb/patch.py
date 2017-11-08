# 3p
import wrapt
import MySQLdb

# project
from ddtrace import Pin
from ddtrace.contrib.dbapi import TracedConnection
from ...ext import net, db


KWARGS_BY_TAG = {
    net.TARGET_HOST: 'host',
    net.TARGET_PORT: 'port',
    db.USER: 'user',
    db.NAME: 'db',
}

KW_BY_POS = {
    0: 'host',
    1: 'user',
    3: 'db',
    4: 'port',
}

def patch():
    wrapt.wrap_function_wrapper('MySQLdb', 'Connect', _connect)
    # `Connection` and `connect` are aliases for `Connect`, patch them too
    if hasattr(MySQLdb, 'Connection'):
        MySQLdb.Connection = MySQLdb.Connect
    if hasattr(MySQLdb, 'connect'):
        MySQLdb.connect = MySQLdb.Connect

def unpatch():
    if isinstance(MySQLdb.Connect, wrapt.ObjectProxy):
        MySQLdb.Connect = MySQLdb.Connect.__wrapped__
        if hasattr(MySQLdb, 'Connection'):
            MySQLdb.Connection = MySQLdb.Connect
        if hasattr(MySQLdb, 'connect'):
            MySQLdb.connect = MySQLdb.Connect

def _connect(func, instance, args, kwargs):
    conn = func(*args, **kwargs)
    return patch_conn(conn, *args, **kwargs)

def patch_conn(conn, *args, **kwargs):
    tags = {t: kwargs[k] for t, k in KWARGS_BY_TAG.items() if k in kwargs}
    for p, k in KW_BY_POS.items():
        if k not in tags and len(args) > p:
            tags[k] = args[p]
    pin = Pin(service="mysql", app="mysql", app_type="db", tags=tags)

    # grab the metadata from the conn
    wrapped = TracedConnection(conn)
    pin.onto(wrapped)
    return wrapped
