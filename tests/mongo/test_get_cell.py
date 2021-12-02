from pyg import get_cell, get_data, mongo_table, mul_, cell, dictable, db_cell, dt, get_DAG, add_, cell_push
from pyg.mongo._db_cell import GRAPH, UPDATED
import pytest
from functools import partial
from pyg import * 

def test_get_cell():
    db = mongo_table(db = 'test', table = 'test')
    db.drop()
    d = (dictable(a = [1,2,3,4,5,6]) * dict(b = [1,2,3,4,5]))(data = lambda a, b: a*b)
    db.insert_many([cell(row) for row in d])

    assert get_cell('test','test', a = 1, b = 1).data == 1
    assert get_data('test','test', a = 1, b = 1) == 1
    assert get_cell('test','test', a = 3, b = 4).data == 12
    assert get_data('test','test', a = 3, b = 4) == 12
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 3)    
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 30)    
    db.drop()
    
    
def test_get_cell_with_version_control():
    db = partial(mongo_table, db = 'test', table = 'test', pk = ['a', 'b'])
    db().reset.drop()
    d = (dictable(a = [1,2,3]) * dict(b = [1,2,3]))
    c = db_cell(function = mul_, db = db)
    _ = d[lambda a, b: c(a = a, b = b)()]
    t = dt()

    assert get_cell('test','test', a = 1, b = 1).data == 1
    assert get_data('test','test', a = 1, b = 1) == 1
    assert get_cell('test','test', a = 3, b = 2).data == 6
    assert get_data('test','test', a = 3, b = 2) == 6
    with pytest.raises(ValueError):
        load_cell('test', 'test', a = 3)    
    with pytest.raises(ValueError):
        load_cell('test', 'test', a = 30)    
    db().drop()
    with pytest.raises(ValueError):
        load_cell('test','test', a = 1, b = 1)

    assert get_cell('test','test', a = 1, b = 1, deleted = t).data == 1
    db().reset.drop()


def test_get_cell_fail_on_history():
    db = mongo_table(db = 'test', table = 'test')
    db.drop()
    db.insert_one(dict(a = 1, deleted = dt(2000)))
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 1, deleted = dt(2001))
    db.insert_one(dict(a = 1, b = 1))
    db.insert_one(dict(a = 1, b = 2))
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 1, deleted = dt(2001))


def test_cell_push_pull():
    pk = 'key'
    a = cell(add_, a = 1, b = 2, key = 'a', pk = pk)(mode = -1)
    b = cell(add_, a = a, b = 2, key = 'b', pk = pk)(mode = -1)    
    c = cell(add_, a = a, b = b, key = 'c', pk = pk)(mode = -1)
    d = cell(add_, a = c, b = b, key = 'd', pk = pk)(mode = -1)
    e = cell(add_, a = d, b = b, key = 'e', pk = pk)(mode = -1)
    assert a._address in GRAPH
    assert get_data(key = 'e') == 18
    a.a = 6
    a = a.push()
    assert a.data == 8
    assert e.load().data == 38
    assert get_data(key = 'e') == 38

def test_db_cell_push_pull():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key')
    db().reset.drop()
    a = db_cell(add_, a = 1, b = 2, key = 'a', db = db)(mode = -1)
    b = db_cell(add_, a = a, b = 2, key = 'b', db = db)(mode = -1)    
    c = db_cell(add_, a = a, b = b, key = 'c', db = db)(mode = -1)
    d = db_cell(add_, a = c, b = b, key = 'd', db = db)(mode = -1)
    e = db_cell(add_, a = d, b = b, key = 'e', db = db)(mode = -1)
    assert get_data('test', 'test', key = 'e') == 18
    a.a = 6
    a = a.push()
    assert a.data == 8
    assert get_data('test', 'test', key = 'e') == 38
    assert GRAPH[e._address].data == 38
    b.b = 4
    b = b.push()
    assert GRAPH[e._address].data == 44
    assert e.data == 18
    assert e.load().data == 44
    db().reset.drop()

def test_db_cell_queued_push_pull():
    db = partial(mongo_table, db = 'test', table = 'test', pk = 'key')
    db().reset.drop()
    a = db_cell(add_, a = 1, b = 2, key = 'a', db = db)(mode = -1)
    b = db_cell(add_, a = a, b = 2, key = 'b', db = db)(mode = -1)  
    c = db_cell(add_, a = a, b = b, key = 'c', db = db)(mode = -1)
    d = db_cell(add_, a = c, b = b, key = 'd', db = db)(mode = -1)
    e = db_cell(add_, a = d, b = b, key = 'e', db = db)(mode = -1)
    assert get_data('test', 'test', key = 'e') == 18
    a.a = 6
    a = a.push()
    assert get_data('test', 'test', key = 'e') == 38
    b.b = 4
    b = b.push()
    assert get_data('test', 'test', key = 'e') == 44
    db().reset.drop()
    a = db_cell(add_, a = 1, b = 2, key = 'a', db = db)(mode = -1)
    b = db_cell(add_, a = a, b = 2, key = 'b', db = db)(mode = -1)  
    c = db_cell(add_, a = a, b = b, key = 'c', db = db)(mode = -1)
    d = db_cell(add_, a = c, b = b, key = 'd', db = db)(mode = -1)
    e = db_cell(add_, a = d, b = b, key = 'e', db = db)(mode = -1)
    a.a = 6
    a = a.go()
    cell_push()
    assert get_data('test', 'test', key = 'e') == 38
    b.b = 4
    b = b.go()
    assert len(UPDATED) == 1 and b._address in UPDATED
    cell_push()
    assert get_data('test', 'test', key = 'e') == 44
    assert e.load().data == 44    
    db().reset.drop()

    