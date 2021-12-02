import pandas as pd; import numpy as np
from functools import partial
from pyg.base._cell import _pk
from pyg import db_cell, cell, dt, mongo_table, eq, GRAPH, presync, pd_read_parquet, drange, \
    parquet_write, encode, db_save, db_load, cell_clear, load_cell, load_data, \
    v2na, add_, sub_, div_, ewma, ewmrms

from operator import add
from functools import partial
from pyg import *
import pytest

updated = 'updated'
f = add_

millisec = dt.timedelta(microseconds = 1000)

def test_db_cell_save():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key')    
    d = db()    

    d.reset.drop()
    self = db_cell(pd.Series, data  = dict(a=1,b=2), output = 'a', db = db, key = 'a')
    self = self.go()
    
    res = d.inc(key = 'a')[0] - _pk
    assert eq(res - updated, self - updated)

    a = pd.DataFrame(dict(a = [1,2,3], b= [4,5,6])); b = 3
    self = db_cell(presync(f), a = a, b = b, db = db, key = 'b')
    self = self.go()
    res = d.inc(key = 'b')[0] - _pk
    assert eq(res - updated, self - updated)
    assert self.updated - res.updated < millisec
    d.reset.drop()

def test_db_cell_save_root():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key', writer = 'c:/temp/%key.parquet')    
    d = db()    
    d.reset.drop()
    a = pd.DataFrame(dict(a = [1,2,3], b= [4,5,6]), index = drange(2)); b = pd.DataFrame(np.random.normal(0,1,(3,2)), columns = ['a','b'], index = drange(2))
    self = db_cell(presync(f), a = a, b = b, db = db, key = 'b')
    self = self.go()
    res = d.inc(key = 'b')[0] - _pk
    assert eq(res - updated , self - updated )
    assert self.updated - res.updated < millisec
    path = 'c:/temp/b/data.parquet'
    assert eq(pd_read_parquet(path), res.data)    
    res = db_cell(db = db, key = 'b').load()
    assert eq(res.data, self.data)    
    GRAPH = {} # not from cache please
    res = db_cell(db = db, key = 'b').load() - _pk
    assert eq(res.data, self.data)    

def test_db_save():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key')    
    missing = db_cell(data = 1, db = db)
    d = db()    
    assert db_save(missing) == missing
    d.reset.drop()
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


def test_db_cell_clear():
    db = partial(mongo_table, db = 'temp', table = 'temp', pk = 'key')    
    d = db()    
    d.reset.drop()
    a = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'a', db = db)
    b = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'b', db = db)
    c = db_cell(lambda a, b: a+b, a = a, b = b, key = 'c', db = db)
    c = c()
    
    assert cell_clear(a) == db_cell(db = db, key = 'a')
    assert cell_clear(b) == db_cell(db = db, key = 'b')
    assert cell_clear(c) == db_cell(db = db, key = 'c')
    assert cell_clear([a,b,c]) == [cell_clear(a), cell_clear(b), cell_clear(c)]
    assert cell_clear((a,b,c)) == (cell_clear(a), cell_clear(b), cell_clear(c))
    assert cell_clear(dict(a=a,b=b,c=c)) == dict(a=cell_clear(a), b=cell_clear(b), c=cell_clear(c))
    assert cell_clear(5) == 5
    
    assert db_cell(5) == db_cell(5, db = None)
    assert a._address == (('url', 'localhost:27017'), ('db', 'temp'), ('table', 'temp'), ('key', 'a'))
    assert db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'a', pk = 'key')._address == (('key', 'a'),)


def test_db_cell_clear_mix():
    db = partial(mongo_table, db = 'temp', table = 'test_db_cell_clear_mix', pk = 'key')    
    db().reset.drop()
    a = db_cell(add_, a = 1, b = 2, key = 'a', db = db)
    b = db_cell(add_, a = 1, b = 2, key = 'b', pk = ['key'])
    c = db_cell(add_, a = a, b = b, key = 'c')
    d = db_cell(add_, a = a, b = c, key = 'd', db = db)
    d = d(0,-1)
    x = db().inc(key = 'd')[0]     
    assert 'data' not in x.b and 'data' not in x.a
    assert x.a.load().data == 3
    y = x.go(3)
    assert y.data == d.data
    db().reset.drop()


def test_db_cell_address():
    pass
    c = db_cell(lambda a, b: a+b, a = 1, b = 2)
    assert c._address == None
    assert c().data == 3
    
def test_db_load():
    db = partial(mongo_table, db = 'temp', table = 'test_db_load', pk = 'key')    
    d = db()    
    d.reset.drop()
    a = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'a', db = db)
    assert a.run()
    b = db_cell(lambda a, b: a+b, a = 1, b = 2, key = 'b', db = db)
    assert b.run()
    self = db_cell(lambda a, b: a+b, a = a, b = b, key = 'c', db = db)
    self = self()
    GRAPH[b._address]
    b()
    b.load()
    assert b.load().data == 3
    assert db_cell(db = db, key = 'b').load().data == 3
    assert db_cell(db = db, key = 'wrong').load() == db_cell(db = db, key = 'wrong')
    assert db_cell(db = db, key = 'b').load(-1) == db_cell(db = db, key = 'b')
    assert db_cell(db = db, key = 'wrong').load(-1) == db_cell(db = db, key = 'wrong')


    a_ = db_cell(key = 'a', db = db)
    self = b_ = db_cell(key = 'b', db = db)
    
    c_ = db_cell(key = 'c', db = db)
    
    e = db_cell(db = db)
    assert e.load() == e
    assert db_load(e) == e
    
    assert db_load(3) == 3
    assert db_load(a_).data == 3
    assert db_load(b_).data == 3
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


def test_db_cell_cache_on_cell_func():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key')    
    db().reset.drop()
    a = db_cell(dt, key = 'a', db = db)    
    b = cell(lambda x: x, x=a)
    c = cell(lambda x: x, x=a)
    b = b()
    c = c()
    assert c.data == b.data
    b = db_cell(lambda x: x, data = a)
    

def add1(x):
    return x+1

def test_db_cell_bare():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key')
    db().reset.drop()
    c = db_cell()
    assert cell_clear(c) == c
    c = db_cell(lambda x: x+1, x = 1)
    assert c().data == 2
    assert c()._clear() - updated  == c
    c = db_cell(data = 1)    
    d = db_cell(lambda x: x+1, x = c, db = db, key = 'key')
    d = d()
    assert db().inc(key = 'key')[0].x - updated == c - updated
    c = db_cell(add1, x = 3)()
    assert cell_clear(c) == c- 'data'
    c = cell(add1, x = 3)
    d = db_cell(add1, x = c, db = db, key = 'key2')
    d = d()
    assert (db().inc(key = 'key2')[0] - 'data').go(1).data == 5
    db().reset.drop()

    
def fake_ts(ticker):
    return pd.Series(np.random.normal(0,1,1000), drange(-999))

def test_db_cell_network():
    db = partial(mongo_table, db = 'test', table = 'test', pk = ['key'])
    db().reset.drop()
    appl = db_cell(fake_ts, ticker = 'appl', key = 'appl_rtn', db = db)()
    a = cell(ewma, a = appl, n = 30)
    b = cell(ewma, a = appl, n = 50)
    c = db_cell(sub_, a = a, b = b, key = 'calculate difference of ewma', pk = 'key')
    d = db_cell(ewmrms, a = c, n = 100, key = 'root mean square of difference', pk = 'key')
    e = db_cell(v2na, a = d, key = 'change zero to nan before dividing by rms', pk = 'key')
    f = db_cell(div_, a = c, b = e, key = 'appl_crossover', db = db)()
    assert db().key == ['appl_crossover', 'appl_rtn']
    loaded = db()[dict(key = 'appl_crossover')]
    assert not 'data' in loaded.b
    assert not 'data' in loaded.a
    assert eq(loaded.data, f.data)
    assert eq((loaded -'data').go().data, f.data)
    db().reset.drop()
    
    
def test_db_cell_point_in_time():
    db = partial(mongo_table, db = 'test', table = 'test', pk = ['key'])
    db().reset.drop()

    ## pre 2000
    x0 = db_cell(add_, a = 1, b = 2, key = 'x', db = db).go()
    y0 = db_cell(add_, a = x0, b = 2, key = 'y', db = db).go()
    z0 = db_cell(add_, a = x0, b = y0, key = 'z', db = db).go()

    ## now delete
    db().reset.inc(q.deleted.not_exists).set(deleted = dt(2000))
    assert len(db()) == 0

    ## now try and grab...
    with pytest.raises(ValueError):
        load_cell('test', 'test', key = 'z')

    assert load_cell('test', 'test', key = 'z', deleted = dt(1999)).data == 8
    assert get_cell('test', 'test', key = 'z', deleted = dt(1999)).data == 8

    with pytest.raises(ValueError):
        load_cell('test', 'test', key = 'z', deleted = dt(2001))

    x1 = db_cell(add_, a = 10, b = 20, key = 'x', db = db)(mode = -1)
    y1 = db_cell(add_, a = x1, b = 20, key = 'y', db = db)(mode = -1)
    z1 = db_cell(add_, a = x1, b = y1, key = 'z', db = db)(mode = -1)
    
    db().reset.inc(q.deleted.not_exists).set(deleted = dt(2002))

    assert get_cell('test', 'test', key = 'z', deleted = dt(1999)).data == 8
    assert get_cell('test', 'test', key = 'z', deleted = dt(2001)).data == 80

    x2 = db_cell(add_, a = 100, b = 200, key = 'x', db = db)(mode = -1)
    y2 = db_cell(add_, a = x2, b = 200, key = 'y', db = db)(mode = -1)
    z2 = db_cell(add_, a = x2, b = y2, key = 'z', db = db)(mode = -1)
        
    assert get_cell('test', 'test', key = 'z', deleted = dt(1999)).data == 8
    assert get_cell('test', 'test', key = 'z', deleted = dt(2001)).data == 80
    assert get_cell('test', 'test', key = 'z').data == 800

    
    assert db_cell(db = db, key = 'z').load(-1).load(mode = dt(1999)).data == 8
    assert db_cell(db = db, key = 'z').load(mode = [dt(2001)]).data == 80
    assert db_cell(db = db, key = 'z').load(mode = [0]).data == 800
    assert db_cell(db = db, key = 'z').load(mode = []).data == 800

    t0 = dt() ### This will be useful shortly...

    full_recalc_using_1999_data = db_cell(db = db, key = 'z')(go=-1, mode = [dt(1999)])
    assert full_recalc_using_1999_data.data == 8

    full_recalc_using_2001_data = db_cell(db = db, key = 'z')(go=-1, mode = [dt(2001)])
    assert full_recalc_using_2001_data.data == 80
    
    ## Be aware, now that we fully recalculated, the current value IS the same as the values in 2001    
    full_recalc_using_live_data = db_cell(db = db, key = 'z')(go=-1, mode = [0])
    assert full_recalc_using_live_data.data == 80
    
    full_recalc_using_live_data_before_we_messed_with_it = db_cell(db = db, key = 'z')(go=-1, mode = [t0])
    assert full_recalc_using_live_data_before_we_messed_with_it.data == 800    
    db().reset.drop()    
    
