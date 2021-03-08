from pyg import pd_read_parquet, pd_to_parquet, mongo_table, eq,drange, dt, parquet_encode, dictable, alphabet, Dict, timer
import numpy as np
import pandas as pd
import pytest

def test_pd_to_parquet():
    path = 'c:/temp/temp.parquet'
    values = [pd.DataFrame(dict(a=[1,2,np.nan])), 
              pd.DataFrame([1,2,3]),
              pd.DataFrame([[1,'a'], [2, 'b']]),
              pd.DataFrame([[1,'a'], [2, 'b']], index = drange(-1)),
              pd.Series([1,2,3]), 
              pd.Series([1.,np.nan,3]),
              pd.Series([1.,np.nan,3], drange(-2)),
              pd.Series(dict(a =1, b='b', c=np.datetime64(dt(0)))),
              pd.Series(dict(a =[1,2], b='b', c=np.datetime64(dt(0))))
              ]
    for value in values:
        fn = pd_to_parquet(value, path)
        df = pd_read_parquet(fn)
        assert eq(df, value)

    assert pd_to_parquet(5, 'whatever') == 5


def test_read_parquet():
    assert pd_read_parquet('no good path') is None


def test_parquet_encode():
    path = 'c:/temp'
    value = dict(key = 'a', n = np.random.normal(0,1, 10), data = dictable(a = [pd.Series([1,2,3]), pd.Series([4,5,6])], b = [1,2]), other = dict(df = pd.DataFrame(dict(a=[1,2,3], b= [4,5,6]))))
    encoded = parquet_encode(value, path)
    assert encoded['n']['file'] == 'c:/temp/n.npy'
    assert encoded['data'].a[0]['path'] == 'c:/temp/data/a/0.parquet'
    assert encoded['other']['df']['path'] == 'c:/temp/other/df.parquet'


def test_parquet_encode_timing():
    df = pd.DataFrame(np.random.normal(0,1,(10000, 26)), columns  = list(alphabet), index = drange(-9999))
    d = Dict({k: df[k].values for k in df.columns})
    d.index = df.index

    p = mongo_table('test', 'test', writer = 'c:/temp.parquet')

    doc_df = dict(key = 'a', data = df)
    doc_ts = dict(key = 'a', data = d)

    _ = timer(lambda doc: p.insert_one(doc), n = 10)(doc_df)
    _ = timer(lambda doc: p.insert_one(doc), n = 10)(doc_ts)
    