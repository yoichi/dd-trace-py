# 3p
import wrapt
import MySQLdb

# project
from ddtrace import Pin
from ddtrace.contrib.dbapi import TracedConnection
from ...ext import net, db


CONN_ATTR_BY_TAG = {
    net.TARGET_HOST : 'server_host',
    net.TARGET_PORT : 'server_port',
    db.USER: 'user',
    db.NAME: 'database',
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
    return patch_conn(conn)

def patch_conn(conn):

    tags = {t: getattr(conn, a) for t, a in CONN_ATTR_BY_TAG.items() if getattr(conn, a, '') != ''}
    pin = Pin(service="mysql", app="mysql", app_type="db", tags=tags)

    # grab the metadata from the conn
    wrapped = TracedConnection(conn)
    pin.onto(wrapped)
    return wrapped
