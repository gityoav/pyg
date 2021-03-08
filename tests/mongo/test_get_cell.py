from pyg import get_cell, get_data, mongo_table, mul_, cell, dictable, db_cell, dt
import pytest
from functools import partial

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
    db().raw.drop()
    d = (dictable(a = [1,2,3]) * dict(b = [1,2,3]))
    c = db_cell(function = mul_, db = db)
    _ = d[lambda a, b: c(a = a, b = b)()]
    t = dt()

    assert get_cell('test','test', a = 1, b = 1).data == 1
    assert get_data('test','test', a = 1, b = 1) == 1
    assert get_cell('test','test', a = 3, b = 2).data == 6
    assert get_data('test','test', a = 3, b = 2) == 6
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 3)    
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 30)    
    db().drop()
    with pytest.raises(ValueError):
        get_cell('test','test', a = 1, b = 1)

    assert get_cell('test','test', a = 1, b = 1, _deleted = t).data == 1
    db().raw.drop()


def test_get_cell_fail_on_history():
    db = mongo_table(db = 'test', table = 'test')
    db.drop()
    db.insert_one(dict(a = 1, _deleted = dt(2000)))
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 1, _deleted = dt(2001))
    db.insert_one(dict(a = 1, b = 1))
    db.insert_one(dict(a = 1, b = 2))
    with pytest.raises(ValueError):
        get_cell('test', 'test', a = 1, _deleted = dt(2001))
