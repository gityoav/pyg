from pyg import mongo_table, dictable, Dict, passthru
from pyg import * 
import pytest

def test_mongo_reader_allows_passthru():
    c = mongo_table('test', 'test')
    assert c._reader(False) == [passthru]    
    assert c._reader(passthru) == [passthru]    

def test_mongo_cursor():
    c = mongo_table('test', 'test')
    c.drop()    
    docs = dictable(a = range(10)) * dict(b = range(10))
    c = c.insert_many(docs)
    assert len(c) == 100
    assert len(c.a) == 10
    assert len(c.b) == 10
    c = c.set(ab = lambda a, b: a*b)
    assert len(c.ab) == 37
    assert len(c.find(ab = 12)) == 4 ## 4 * 3 and 6 * 2
    assert len(c.find_one(a = 0, b = 0)) == 1
    c = c.delete_one(a = 0, b = 0)
    with pytest.raises(ValueError):
        c.find_one(a = 0, b = 0)
    
    doc = c.find_one(a = 1, b = 1)[0]
    doc['sum'] = 2
    c.update_one(doc)
    new_doc = c.find_one(a = 1, b = 1)[0]
    assert new_doc.sum == 2
    
    doc = dict(status = 'test')
    c = c.update_many(doc)
    doc = c.find_one(a = 5, b = 2)[0]
    assert doc.status == 'test'
    del c['status']
    doc = c.find_one(a = 5, b = 2)[0]
    assert 'status' not in doc  
    
    c['status'] = 'test2'
    doc = c.find_one(a = 5, b = 2)[0]
    assert doc.status == 'test2'


    c = c.set(status = 'test3')
    doc = c.find_one(a = 5, b = 2)[0]
    assert doc.status == 'test3'

    c = c.set(status = 'test4', sum = lambda a,b:a+b)
    doc = c.find_one(a = 5, b = 2)[0]
    assert doc.status == 'test4'
    assert doc.sum == 7    
    
    doc.status = 'test5' ## unlike normal insert, we overwrite since _id in doc
    c.insert_one(doc)
    assert len(c) == 99
    doc = c.find_one(a = 5, b = 2)[0]
    assert doc.status == 'test5'

    del doc['_id']
    doc.status = 'test6: no id so new doc'    
    c.insert_one(doc)
    assert len(c.find(a = 5, b = 2)) == 2
    c.inc(status = 'test6: no id so new doc').drop()
    assert len(c) == 99
    doc = c.find_one(a = 5, b = 2)[0]
    assert doc.status == 'test5'
    
    t = c[::]
    t = t(power = lambda a, b: (a+1)**(b-1))
    ### ids exist so should update
    c.insert_many(t)
    assert len(c) == 99
    doc = c.inc(power = 5)[0]
    assert doc.a == 4 and doc.b == 2
    
    t = t - '_id' ## now we add new docs
    c.insert_many(t)
    assert len(c) == 99 + len(t)
    c = c.drop()
    c = c + t
    assert len(c) == len(t)    

    assert Dict(c.sort('a', 'b')[['a','b']][0]) - '_id' == Dict(a = 0, b = 1)
    del c.sort('a', 'b')[0]
    self = c.sort('a', 'b') 

    with pytest.raises(ValueError):
        c.find_one(a = 0, b = 1)

    del c['sum']
    assert 'sum' not in c[0]

    del c[dict(a = 1, b = 2)]
    with pytest.raises(ValueError):
        c.find_one(a = 1, b = 2)

    with pytest.raises(ValueError):
        del c[dict(a = 1, b = 2)]
        
    c + dict(x = 1, y = 2)
    assert c.x == [1]
    c.drop()


def test_cursor_bits():
    c = mongo_table('test', 'test')
    c =  c.drop()
    d = dictable(a = [1,2,3]) * dict(b = [1,2,3])
    c = c.insert_many(d)
    assert c.docs() == dictable(doc = list(c))
    assert dictable(c) == dictable(c.docs().doc)
    assert sorted(c.keys()) == ['_id', 'a', 'b']
    assert sorted(c(reader = False).keys()) == ['_id', '_obj', 'a', 'b']

