from pyg import mongo_table, dictable, Dict, passthru, waiter
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
    
    
    doc = await c.read_one(a = 1, b = 1)
    doc['sum'] = 2
    await c.update_one(doc)
    new_doc = await c.read_one(a = 1, b = 1)
    assert new_doc.sum == 2
    
    doc = dict(status = 'test')
    c = await c.update_many(doc)
    doc = await c.read_one(a = 5, b = 2)
    assert doc.status == 'test'
    await c.delete('status')
    doc = await c.read_one(a = 5, b = 2)
    assert 'status' not in doc  
    
    await c.set(status = 'test2')
    doc = await c.read_one(a = 5, b = 2)
    assert doc.status == 'test2'


    c = await c.set(status = 'test3')
    doc = await c.read_one(a = 5, b = 2)
    assert doc.status == 'test3'

    c = await c.set(status = 'test4', sum = lambda a,b:a+b)
    doc = await c.read_one(a = 5, b = 2)
    assert doc.status == 'test4'
    assert doc.sum == 7    
    
    doc.status = 'test5' ## unlike normal insert, we overwrite since _id in doc
    await c.insert_one(doc)
    assert await c.count() == 99
    doc = await c.read_one(a = 5, b = 2)
    assert doc.status == 'test5'

    del doc['_id']
    doc.status = 'test6: no id so new doc'    
    await c.insert_one(doc)
    assert await c.find(a = 5, b = 2).count() == 2
    await c.inc(status = 'test6: no id so new doc').drop()
    assert await c.count() == 99
    doc = await c.read_one(a = 5, b = 2)
    assert doc.status == 'test5'
    
    t = await c[::]
    t = t(power = lambda a, b: (a+1)**(b-1))
    ### ids exist so should update
    await c.insert_many(t)
    assert await c.count() == 99
    doc = await c.inc(power = 5)[0]
    assert doc.a == 4 and doc.b == 2
    
    t = t - '_id' ## now we add new docs
    await c.insert_many(t)
    assert await c.count() == 99 + len(t)
    c = await c.drop()

    await c.insert_many(t)
    cc = await c.sort('a', 'b')[['a', 'b']]
    assert Dict(await cc[0]) - '_id' == Dict(a = 0, b = 1)
    await c.sort('a', 'b').delete(0)

    with pytest.raises(ValueError):
        await c.find_one(a = 0, b = 1)

    await c.delete('sum')
    assert 'sum' not in await c[0]

    await c.delete(dict(a = 1, b = 2))
    with pytest.raises(ValueError):
        await c.find_one(a = 1, b = 2)

    with pytest.raises(ValueError):
        await c.delete(dict(a = 1, b = 2))
        
    await c.drop()

@pytest.mark.asyncio
async def test_cursor_bits():
    c = mongo_table('test', 'test', asynch = True)
    c =  await c.drop()
    d = dictable(a = [1,2,3]) * dict(b = [1,2,3])
    c = await c.insert_many(d)
    assert await c.docs() == dictable(doc = await waiter([c.read(i) for i in range(await c.count())]))
    # assert dictable(c) == dictable(c.docs().doc)
    assert sorted(await c.keys()) == ['_id', 'a', 'b']
    assert sorted(await c(reader = False).keys()) == ['_id', '_obj', 'a', 'b']

