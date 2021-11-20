from pyg import mongo_table, dictable, Dict, passthru
from pyg import * 
import pytest

def test_async_mongo_reader_allows_passthru():
    c = mongo_table('test', 'test', asynch = True)
    assert c._reader(False) == [passthru]    
    assert c._reader(passthru) == [passthru]    

@pytest.mark.asyncio
async def test_async_mongo_cursor():
    c = mongo_table('test', 'test', mode = 'a')
    await c.drop()    
    docs = dictable(a = range(10)) * dict(b = range(10))
    c = await c.insert_many(docs)
    assert await c.count() == 100
    assert len(await c.distinct('a')) == 10
    assert len(await c.distinct('b')) == 10
    c = await c.set(ab = lambda a, b: a*b)
    assert len(await c.distinct('ab')) == 37
    assert await c.find(ab = 12).count() == 4 ## 4 * 3 and 6 * 2
    assert await (await c.find_one(a = 0, b = 0)).count() == 1
    c = await c.delete_one(a = 0, b = 0)
    with pytest.raises(ValueError):
        await c.find_one(a = 0, b = 0)
    
