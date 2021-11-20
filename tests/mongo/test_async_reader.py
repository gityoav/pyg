from pyg import mongo_table, mongo_reader, dt, eq, mongo_cursor, passthru, mongo_async_reader
import pytest
import pandas as pd




def test_async_cursor_reader_writer():
    cursor = mongo_table('test', 'test', mode = 'ar', writer = False, reader = False)
    assert cursor._writer() == [passthru]
    assert cursor._reader() == [passthru]


def test_async_mongo_reader_spec_projection():
    reader = mongo_table('test', 'test', mode = 'ar')
    assert reader.projection is None    
    assert reader(projection = ['a','b']).projection == ['a', 'b']   
    assert not reader.spec 
    assert reader.find(a = 1).spec == {"a": {"$eq": 1}}  
    assert reader() == reader
    assert reader.project() == reader
    assert reader.exc(a = 1).spec == {"a": {"$ne": 1}}
    assert reader.find(a = 1).spec == {"a": {"$eq": 1}}
    

def test_async_reader_is_no_writer():
    reader = mongo_table('test', 'test', mode = 'ar')
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

@pytest.mark.asyncio
async def test_async_mongo_reader__read():
    t = mongo_table('test', 'test', pk = 'key', mode = 'aw')
    await t.reset.drop()
    df = pd.Series([1,2])
    await t.insert_one(dict(key = 1, df = df))
    reader = mongo_table('test', 'test', mode = 'r', asynch = True)
    raw = await reader.read(0, passthru)
    assert eq(reader._read(raw)['df'], df) ## now converted to df
    pair = await reader.read([0,0])
    assert eq(pair[0]['df'], df) and eq(pair[1]['df'], df)
    assert "MotorCollection(Collection(" in str(reader)
    await t.reset.drop()    



@pytest.mark.asyncio
async def test_async_mongo_reader_distinct():
    t = mongo_table('test', 'test', pk = 'key', asynch = True)
    await t.reset.drop()
    await t.insert_one(dict(key = 'a'))
    await t.insert_one(dict(key = 1))
    await t.insert_one(dict(key = None))
    await t.insert_one(dict(key = dt(0)))
    reader = mongo_table('test', 'test', mode = 'ar')

    keys = await reader.distinct('key')
    for key in [None, dt(0), 1, 'a']:
        assert key in keys
        
    await t.reset.drop()
    await t.insert_one(dict(key = 'a'))
    await t.insert_one(dict(key = 'd'))
    await t.insert_one(dict(key = 'b'))
    await t.insert_one(dict(key = 'e'))
    await t.insert_one(dict(key = 'c'))
    assert await reader.distinct('key') == ['a', 'b', 'c', 'd', 'e']
    await t.reset.drop()

    reader._whatever = 1
    assert reader._whatever == 1

# -*- coding: utf-8 -*-

