import pandas as pd; import numpy as np
from functools import partial
from pyg import db_cell, mongo_table, eq, presync, pd_read_parquet, drange, parquet_write, encode, db_save, db_load, db_ref
from operator import add
from functools import partial
from pyg import *
from pyg.mongo._db_cell import _GRAPH
import pytest

def f(a,b):
    return a+b


def test_db_cell_save():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key')    
    d = db()    
    d.raw.drop()
    self = db_cell(pd.Series, data  = dict(a=1,b=2), output = 'a', db = db, key = 'a')
    self = self.go()
    res = d.inc(key = 'a')[0] - '_pk'
    assert eq(res, self)


    a = pd.DataFrame(dict(a = [1,2,3], b= [4,5,6])); b = 3
    self = db_cell(presync(f), a = a, b = b, db = db, key = 'b')
    self = self.go()
    res = d.inc(key = 'b')[0] - '_pk'
    assert eq(res, self)

    d.raw.drop()

def test_db_cell_save_root():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key', writer = '.parquet', root = 'c:/temp/%key')    
    assert db().writer == [parquet_write, encode]
    d = db()    
    d.raw.drop()
    a = pd.DataFrame(dict(a = [1,2,3], b= [4,5,6]), index = drange(2)); b = pd.DataFrame(np.random.normal(0,1,(3,2)), columns = ['a','b'], index = drange(2))
    self = db_cell(presync(f), a = a, b = b, db = db, key = 'b')
    self = self.go()
    res = d.inc(key = 'b')[0] - '_pk'
    assert eq(res, self)
    path = 'c:/temp/b/data.parquet'
    assert eq(pd_read_parquet(path), res.data)    
    res = db_cell(db = db, key = 'b').load()
    assert eq(res, self)    
    _GRAPH = {} # not from cache please
    res = db_cell(db = db, key = 'b').load() - '_pk'
    assert eq(res, self)    


def test_db_save():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key')    
    missing = db_cell(data = 1, db = db)
    d = db()    
    assert db_save(missing) == missing
    d.raw.drop()
    value = db_cell(data = 1, key = 'a', db = db)
    assert len(d) == 0
    doc = db_save(value)
    assert len(d) == 1
    docs = db_save([value, value])
    assert len(d) == 1 and docs[0] == docs[1] == doc
    docs = db_save(dict(a = value, b = value))
    assert docs['a']==docs['b']
    
    db = partial(mongo_table, db = 'temp', table = 'temp')## non unique PK
    d = db()    
    d = d.drop()
    value = db_cell(data = 1, key = 'a', db = db)
    doc = db_save(value)
    docs = db_save([value, value])
    assert len(d) == 3
    assert docs[0]!=docs[1]
    d = d.drop()
    
    docs = db_save(dict(a = value, b = value))
    assert docs['a']!=docs['b']
    assert db_save(5) == 5
    assert db_save(db_cell(5)) == db_cell(5)


def test_db_ref():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key')    
    d = db()    
    d.raw.drop()
    a = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'a', db = db)
    b = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'b', db = db)
    c = db_cell(lambda a, b: a+b, a = a, b = b, key = 'c', db = db)
    c = c()
    
    assert db_ref(a) == db_cell(db = db, key = 'a')
    assert db_ref(b) == db_cell(db = db, key = 'b')
    assert db_ref(c) == db_cell(db = db, key = 'c')
    assert db_ref([a,b,c]) == [db_ref(a), db_ref(b), db_ref(c)]
    assert db_ref((a,b,c)) == (db_ref(a), db_ref(b), db_ref(c))
    assert db_ref(dict(a=a,b=b,c=c)) == dict(a=db_ref(a), b=db_ref(b), c=db_ref(c))
    assert db_ref(5) == 5
    
    assert db_cell(5) == db_cell(5, db = None)
    assert a._address == ('localhost', 27017, 'temp', 'temp', ('key',), ('a',))
    assert db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'a', db = 'key')._address == (None, None, None, None, ('key',), ('a',))


def test_db_cell_address():
    pass
    c = db_cell(lambda a, b: a+b, a = 1, b = 2)
    assert c._address == None
    assert c().data == 3
    
def test_db_load():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key')    
    d = db()    
    d.raw.drop()
    a = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'a', db = db)
    b = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'b', db = db)
    c = db_cell(lambda a, b: a+b, a = a, b = b, key = 'c', db = db)
    c = c()
    assert b.load().data == 3
    assert db_cell(db = db, key = 'b').load().data == 3
    assert db_cell(db = db, key = 'wrong').load() == db_cell(db = db, key = 'wrong')
    assert db_cell(db = db, key = 'b').load(-1) == db_cell(db = db, key = 'b')
    assert db_cell(db = db, key = 'wrong').load(-1) == db_cell(db = db, key = 'wrong')

    a_ = db_cell(key = 'a', db = db)
    b_ = db_cell(key = 'b', db = db)
    c_ = db_cell(key = 'c', db = db)
    
    d = db_cell(db = db)
    assert d.load() == d
    assert db_load(d) == d
    
    
    assert db_load(3) == 3
    assert db_load(a_).data == 3
    assert [_.data for _ in db_load([a_,b_])] == [3,3]
    assert {k:v.data for k,v in db_load(dict(a=a_,c=c_)).items()} == dict(a = 3, c = 6)

    assert db_cell(db = db, key = 'wrong').load() == db_cell(db = db, key = 'wrong')
    assert db_cell(db = db, key = 'b').load(-1) == db_cell(db = db, key = 'b')
    assert db_cell(db = db, key = 'wrong').load(-1) == db_cell(db = db, key = 'wrong')

    assert db_cell(db = db, key = 'b').load(1).data == 3
    with pytest.raises(ValueError):
        db_cell(db = db, key = 'wrong').load(1)

    assert db_cell(5).load() == db_cell(5)
    assert db_load(db_cell(5), 0) == db_cell(5)

