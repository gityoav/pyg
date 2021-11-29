from pyg import mongo_table, mongo_reader, dt, eq, mongo_cursor, passthru, mongo_async_reader
import pytest
import pandas as pd

def test_clone_cursor():
    reader = mongo_table('test', 'test', mode = 'r', asynch = True)
    assert isinstance(reader, mongo_async_reader)
    reader = mongo_table('test', 'test', mode = 'r')
    assert isinstance(reader, mongo_reader)
    assert mongo_reader(reader) == reader
    cursor = mongo_cursor(reader)
    assert isinstance(cursor, mongo_cursor)


def test_cursor_reader_writer():
    cursor = mongo_table('test', 'test', mode = 'r', writer = False, reader = False)
    assert cursor._writer() == [passthru]
    assert cursor._reader() == [passthru]


def test_mongo_reader_spec_projection():
    reader = mongo_table('test', 'test', mode = 'r')
    assert reader.projection is None    
    assert reader[['a','b']].projection == ['a', 'b']   
    assert not reader.spec 
    assert reader.find(a = 1).spec == {"a": {"$eq": 1}}  
    assert reader() == reader
    assert reader.project() == reader
    assert reader.exc(a = 1).spec == {"a": {"$ne": 1}}
    assert reader.find(a = 1).spec == {"a": {"$eq": 1}}
    

def test_reader_is_no_writer():
    reader = mongo_table('test', 'test', mode = 'r')
    with pytest.raises(AttributeError):
        reader.insert_one(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.insert_many(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.delete_one(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.delete_many(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.set(dict(a =1))    
    with pytest.raises(AttributeError):
        reader.drop(dict(a =1))    

def test_mongo_reader__read():
    t = mongo_table('test', 'test', pk = 'key')
    t.reset.drop()
    df = pd.Series([1,2])
    t.insert_one(dict(key = 1, df = df))
    reader = mongo_table('test', 'test', mode = 'r')
    raw = reader.read(0, passthru)
    assert eq(reader._read(raw)['df'], df) ## now converted to df
    pair = reader.read([0,0])
    assert eq(pair[0]['df'], df) and eq(pair[1]['df'], df)
    assert "Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'test'), 'test')" in str(reader)
    t.reset.drop()    
    assert "documents count: 0" in str(reader)


def test_mongo_reader_distinct():
    t = mongo_table('test', 'test', pk = 'key')
    t.reset.drop()
    t.insert_one(dict(key = 'a'))
    t.insert_one(dict(key = 1))
    t.insert_one(dict(key = None))
    t.insert_one(dict(key = dt(0)))
    reader = mongo_table('test', 'test', mode = 'r')

    keys = reader.key
    for key in [None, dt(0), 1, 'a']:
        assert key in keys
        
    t.reset.drop()
    t.insert_one(dict(key = 'a'))
    t.insert_one(dict(key = 'd'))
    t.insert_one(dict(key = 'b'))
    t.insert_one(dict(key = 'e'))
    t.insert_one(dict(key = 'c'))
    assert reader.key == ['a', 'b', 'c', 'd', 'e']
    t.reset.drop()

    reader._whatever = 1
    assert reader._whatever == 1
