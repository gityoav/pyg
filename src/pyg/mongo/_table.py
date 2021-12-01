from pyg.base import is_str, cfg_read
from pyg.mongo._reader import mongo_reader
from pyg.mongo._cursor import mongo_cursor, mongo_pk_cursor

from pyg.mongo._async_reader import mongo_async_reader
from pyg.mongo._async_cursor import mongo_async_cursor, mongo_async_pk_cursor

from pymongo import MongoClient
from motor import MotorClient

__all__ = ['mongo_table']


def _url(url):
    """
    converts the URL address to actual url based on the cfg['mongo'] locations. see cfg_read() for help.
    """
    cfg = cfg_read()
    mongo = cfg.get('mongo', {})
    _url = url or 'null'
    if _url in mongo:
        return mongo[_url]
    else:
        return url
    

def mongo_table(table, db, pk = None, url = None, reader = None, writer = None, mode = 'w', asynch = False, **kwargs):    
    """
    :Example:
    ---------
    from pyg import *
    from pymongo import MongoClient
    from motor import MotorClient

    table = db = 'test'
    pk = 'key'
    url = reader = writer = None    
    mode = 'aw'    
    kwargs = {}
    client = MotorClient(url)
    c = client[db][table]

    isinstance(cursor, ())
    res = obj(c, pk = pk, writer = writer, reader = reader, **kwargs)
    
    await c.insert_one(dict(a = 1))
    await c.create_index(dict(a = 1))

    cfg = cfg_read()
    
    """ 
    if mode is None:
        mode = 'w'
    if is_str(mode):
        mode = mode.lower()
        if not asynch:
            if mode == '' or mode.startswith('w'):
                obj = mongo_cursor if pk is None else mongo_pk_cursor 
            elif mode.startswith('r'):
                obj = mongo_reader
            else:
                raise ValueError('please specify read/write for mode')
        else:
            if mode == '' or mode.startswith('w'):
                obj = mongo_async_cursor if pk is None else mongo_async_pk_cursor 
            elif mode.startswith('r'):
                obj = mongo_async_reader 
            else:
                raise ValueError('please specify read/write for mode')
    else:
        obj = mode

    url = _url(url)
    if asynch or isinstance(obj, (mongo_async_reader, mongo_async_cursor)):
        client = MotorClient(url)
    else:
        client = MongoClient(url)

    c = client[db][table]
    res = obj(c, pk = pk, writer = writer, reader = reader, **kwargs)
    if isinstance(res, (mongo_reader)) and len(res) == 0:
         res.create_index()
    return res

        
