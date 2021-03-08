from pyg.base import is_str
from pyg.mongo._reader import mongo_reader
from pyg.mongo._cursor import mongo_cursor
from pyg.mongo._pk_reader import mongo_pk_reader
from pyg.mongo._pk_cursor import mongo_pk_cursor
from pymongo import MongoClient

__all__ = ['mongo_table']

def mongo_table(table, db, pk = None, url = None, reader = None, writer = None, mode = 'w', **kwargs):
    if is_str(mode):
        mode = mode.lower()
        if mode.startswith('r'):
            obj = mongo_reader if pk is None else mongo_pk_reader
        elif mode.startswith('w'):
            obj = mongo_cursor if pk is None else mongo_pk_cursor
        else:
            raise ValueError('please specify read/write for mode')
    else:
        obj = mode
    c = MongoClient(url)[db][table]
    res = obj(c, pk = pk, writer = writer, reader = reader, **kwargs)
    if len(res) == 0 and isinstance(res, mongo_pk_reader):
        res.create_index()
    return res

        
    
