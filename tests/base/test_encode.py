from pyg import encode, decode, dt, dt2str, eq, bson2pd, ymd
import jsonpickle as jp
import re
import enum
import numpy as np
import pandas as pd

def test_decode_null():
    assert decode('null') is None


def test_jsonpickle_fail_decode():
    fake = '{"py/function": "pyg.base._as_list.fake"}'
    assert jp.decode(fake) is None
    assert decode(fake) is fake


def test_decode_iso():    
    t = dt(2020,1,1,10,20,30)
    x = encode(dict(a = dt2str(t)))
    assert decode(x, date = 'iso')['a'] == t
    x = encode(dict(a=1))
    assert decode(x, date = 'iso') == dict(a = 1)
    x = encode(dict(a = dt2str(t, '-')))
    date = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}')
    assert decode(x, date = date) == dict(a = ymd(t))


def test_encode():
    e = enum.Enum('ab', ['a', 'b'])
    assert encode(e.a) == 1 and encode(e.b) == 2

    class Test():
        def _encode(self):
            return 'well done'

    assert encode(Test()) == 'well done'
    assert encode(np.int64(32)) == 32 and type(encode(np.int64(32))) == int
    assert encode(np.bool_(True)) == True and type(encode(np.bool_(True))) == bool
    a = np.array([1,2])
    b = encode(a)
    assert sorted(b.keys()) == sorted(['data', 'shape', 'dtype', '_obj'])
    assert eq(decode(b), a)
    
    a = np.array(['afhjlfdas', 'b', dt()])
    b = encode(a)
    assert sorted(b.keys()) == sorted(['data', '_obj'])
    assert eq(bson2pd(b['data']), a)
    assert eq(decode(b), a)

    def f(x):
        return x
    f.cache = dict(a = 'a', b = 'b')
    assert 'cache' not in encode(f) 
    assert f.cache == dict(a = 'a', b = 'b')

    a = pd.Series([1,2,3, np.nan])
    b = encode(a)
    assert sorted(b.keys()) == sorted(['data', '_obj'])
    assert eq(bson2pd(b['data']), a)
    assert eq(decode(b), a)
