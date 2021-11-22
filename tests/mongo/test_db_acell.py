from pyg import *
from pyg import add_, mongo_table, UPDATED, get_data, db_acell, acell_push
from functools import partial
import pytest

@pytest.mark.asyncio
async def test_db_acell_simple():
    db = partial(mongo_table, db = 'db', table = 'table', pk = 'key')    
    a = db_acell(add_, a = 5, b = 2, pk = 'key', key = 'a', db = db)
    b = db_acell(add_, a = a, b = a, pk = 'key', key = 'b', db = db)
    c = db_acell(add_, a = a, b = b, pk = 'key', key = 'c', db = db)
    c = await c.go(-1)    
    assert len(UPDATED) == 3
    await acell_push()
    assert len(UPDATED) == 0
    assert get_data('table', 'db', key = 'c') == 21
    a = db_acell(add_, a = 5, b = 3, pk = 'key', key = 'a', db = db)
    a = await a.push()
    assert get_data('table', 'db', key = 'c') == 24
