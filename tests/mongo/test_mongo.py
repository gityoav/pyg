from pyg.base import ulist, dictable,Dict, pd_read_parquet, eq
from pyg.mongo import mongo_reader, mongo_cursor, q, mongo_table, mongo_pk_cursor, mongo_pk_reader
from pyg import *
import numpy as np; import pandas as pd
import jsonpickle as jp
import pytest

    
def test_mongo_cursor():
    t = mongo_table('test', 'test')
    t.drop()
    t.insert_one(dict(a=1,b=2))
    assert len(t) == 1
    t.insert_one(dict(a=2,b=3))
    assert len(t) == 2    
    res = t[::]    

    assert isinstance(res, dictable) and len(res) == 2
    assert res['a','b'][0] == (1, 2)
    assert res['a','b'][1] == (2, 3)
    
    assert 'b' not in t['a'][::]
    assert len(t.inc(a =1)) == 1
    t.drop()

def test_mongo_pk_cursor():
    t = mongo_table('test', 'test')
    t.drop()
    self  = mongo_table('test', 'test', pk = 'key')    
    assert len(self) == 0
    self.update_one(dict(key = 1, data = 1))
    assert len(self) == 1
    assert len(t) == 1    
    
    doc = dict(key = 1, data = 2)
    self.update_one(doc)
    assert len(self) == 1
    assert len(t) == 2

    doc = dict(key = 1, other_data = 3)
    self.update_one(doc)
    assert len(self) == 1
    assert len(t) == 3
    
    assert self[dict(key=1)]['other_data'] == 3
    assert self[dict(key=1)]['data'] == 2
    self.raw.drop()
    assert len(t) == 0


def test_mongo_pk_cursor_multiple_keys():
    t = mongo_table('test', 'test')
    t.drop()
    self  = mongo_table('test', 'test', pk = ['a','b'])    
    with pytest.raises(ValueError):
        self.insert_one(dict(a=1,c=2))    
    self.insert_one(dict(a=1,b=2))
    assert type(self[0]) == dict
    doc = Dict(a=1,b=2)
    self._write(doc)
    self.update_one(doc)
    assert type(self[0]) == Dict
    self.raw.drop()

    
    
def test_mongo_table_mode():
    with pytest.raises(ValueError):
        mongo_table('test', 'test', mode = 'not good')
        
    r = mongo_table('test', 'test', mode = mongo_reader)
    assert isinstance(r, mongo_reader)


    w = mongo_table('test', 'test', mode = mongo_cursor)
    assert isinstance(w, mongo_cursor)

    with pytest.raises(TypeError):
        mongo_table('test', 'test', mode = mongo_pk_reader)

    with pytest.raises(TypeError):
        mongo_table('test', 'test', mode = mongo_pk_cursor)


def test_mongo_cursor_root():
    t = mongo_table('test', 'test', writer = 'c:/temp/%name/%surname.parquet')
    t.drop()
    doc = dict(name = 'adam', surname = 'smith', ts = pd.Series(np.arange(10)))
    t.insert_one(doc)
    assert eq(pd_read_parquet('c:/temp/adam/smith/ts.parquet'), doc['ts'])
    assert eq(t[0]['ts'], doc['ts'])
    doc = dict(name = 'beth', surname = 'brown', a = np.arange(10))
    t.drop()
    t.insert_one(doc)
    assert eq(np.load('c:/temp/beth/brown/a.npy'), doc['a'])
    assert eq(t[0]['a'], doc['a'])
    t.drop()

    
    