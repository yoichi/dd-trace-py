# 3p
import wrapt
import MySQLdb

# project
from ddtrace import Pin, tracer
from ddtrace.contrib.dbapi import TracedConnection
from ...ext import net, db


KWPOS_BY_TAG = {
    net.TARGET_HOST: ('host', 0),
    db.USER: ('user', 1),
    db.NAME: ('db', 3),
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

@tracer.wrap(service="mysql")
def _connect(func, instance, args, kwargs):
    conn = func(*args, **kwargs)
    tags = {t: kwargs[k] if k in kwargs else args[p]
            for t, (k, p) in KWPOS_BY_TAG.items()
            if k in kwargs or len(args) > p}
    tags[net.TARGET_PORT] = conn.port
    tracer.current_span().set_tags(tags)
    return patch_conn(conn, tags)

def patch_conn(conn, tags):
    pin = Pin(service="mysql", app="mysql", app_type="db", tags=tags)

    # grab the metadata from the conn
    wrapped = TracedConnection(conn)
    pin.onto(wrapped)
    return wrapped
