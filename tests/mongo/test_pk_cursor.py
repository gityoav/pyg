from pyg import mongo_table, dictable, Dict, mongo_pk_cursor, mongo_cursor, mongo_reader, periodic_cell, drange, pd_read_parquet, passthru, eq, dt
from functools import partial
import pytest
import pandas as pd
from pyg.base._cell import _pk

def test_mongo_pk_cursor():
    db = partial(mongo_table, db = 'test', table = 'test', pk = ['a', 'b'])
    t = db()
    d = dictable(a = [1,2,3])*dict(b = [1,2,3])
    t.reset.drop()
    t = t.insert_many(d)
    assert len(t) == 9
    t = t.insert_many(d)
    assert len(t) == 9
    assert len(t.reset) == 18
    t.insert_one(Dict(a = 4, b = 4))
    assert len(t) == 10
    t.insert_one(Dict(a = 4, b = 4))
    assert len(t) == 10
    t.insert_one(Dict(a = 4, b = 4, c = 16))
    assert t.find_one(a = 4, b = 4)[0].c == 16
    t['c'] = lambda a, b: a + b
    assert t.find_one(a = 3, b = 3)[0].c == 6
    assert t.find(a = 3).c == [4,5,6]
    
    del t.sort('a', 'b')[0]
    assert len(t.inc(a = 1, b = 1)) == 0
    del t['c']
    assert 'c' not in t.find_one(a = 3, b = 3)[0]
    assert t.reset.inc(a = 3, b = 3).c == [6]
    del t[dict(a = 3, b = 3)]
    with pytest.raises(ValueError):
        t[dict(a = 3, b = 3)]

    t.set(c = lambda a, b: a+b+5)
    assert t.find_one(a = 1, b = 2)[0].c == 8
    t.reset.drop()


def test_pk_cursor_dedup():
    db = partial(mongo_table, db = 'test', table = 'test', pk = ['a', 'b'])
    r = mongo_table('test', 'test', ['a', 'b'], mode = 'r')
    assert type(r) == mongo_reader
    t = db()
    assert type(t) == mongo_pk_cursor
    d = dictable(a = [1,2,3])*dict(b = [1,2,3])
    t.reset.drop()
    t = t.insert_many(d)
    d[_pk] = [['a', 'b']]
    t.reset.insert_many(d) ## creating duplicates
    t.reset.insert_many(d) ## creating duplicates
    assert len(t) == 27
    assert len(r.find_one(a = 1, b = 1)) == 1
    assert len(t) == 25
    t = t.dedup()
    assert len(t) == 9
    with pytest.raises(KeyError):
        r.find_one(a = 1)


def test_pk_cursor_insert_missing_keys():
    t = mongo_table('test', 'test', pk = ['a', 'b'])
    t.reset.drop()
    with pytest.raises(ValueError):
        t.insert_one(dict(a = 1))


def test_pk_cursor_update_into_insert():
    t = mongo_table('test', 'test', pk = ['a', 'b'])
    t.reset.drop()
    first = t.update_one(dict(a = 1, b = 1))
    second =  t.update_one(dict(a = 1, b = 1))
    assert second['_id'] == first['_id']
    
    
def test_pk_cursor_update_many():
    t = mongo_table('test', 'test', pk = ['a', 'b'])
    t.reset.drop()
    d = dictable(a = [1,2,3], b = [4,5,6])
    res = t.update_many(d)
    assert len(res) == 3 and len(t) == 3 and res[['a', 'b']] == d
    t.reset.drop()
    res = t.update_many(d, False)
    assert res == dictable(data =  [None] * 3)
    
def test_pk_cursor_setitem():
    t = mongo_table('test', 'test', pk = ['a', 'b'])
    t.reset.drop()
    t[dict(a = 1, b = 2)] = 3
    assert len(t) == 1 and t[0]['data'] == 3
    t[dict(a = 1, b = 2)] = dict(c = 3)
    assert len(t) == 1 and t[0]['c'] == 3
    t[dict(a = 2, b = 2)] = dict(c = 4)
    assert len(t) == 2 and t.find_one(a = 2, b = 2)[0]['c']==4


def test_pk_cursor__add__():
    t = mongo_table('test', 'test', pk = ['a', 'b'])
    t.reset.drop()
    d = dictable(a = [1,2,3], b = [4,5,6])
    t = t + d    
    assert len(t) == 3 and t[::][['a', 'b']] == d
    t = t + dict(a = 5, b = 6)
    assert len(t) == 4
    with pytest.raises(ValueError):
        t + 5
        
        
def test_pk_cursor_raw():
    t = mongo_table('test', 'test', pk = ['a', 'b'])
    assert isinstance(t.reset, mongo_cursor) and not t.reset.spec
    r = t.reset.find(a = 1)
    assert isinstance(r.reset, mongo_cursor) and not r.reset.spec

    t = mongo_table('test', 'test', pk = ['a', 'b'], mode = 'r')
    assert isinstance(t.reset, mongo_reader) and not t.reset.spec
    r = t.reset.find(a = 1)
    assert isinstance(r.reset, mongo_reader) and not r.reset.spec 

def test_pk_cursor_dates_to_parquet():
    db = partial(mongo_table, table = 'test', db =  'test', pk = 'a', writer = 'c:/temp/%a/%updated.parquet')    
    db().reset.drop()
    c = periodic_cell(lambda a: pd.Series([1,2,3], drange(-2)), a = 'a', db = db)()
    assert eq(pd_read_parquet(db().read(0, passthru)['data']['path']), c.data)
    db().reset.drop()
    c = periodic_cell(lambda a: pd.Series([1,2,3], drange(-2)), a = 'a', db = db)()
    c.updated = dt(2000)
    c = c.save()
    assert db().read(0, passthru)['data']['path'] == 'c:/temp/a/20000101/data.parquet'
    db().reset.drop()

