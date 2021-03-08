from pyg import Dict,pd_read_parquet, parquet_write, mongo_table, dictable, eq, passthru, cell, drange, root_path, dt, parquet_encode, csv_encode
import pandas as pd
import pytest
from functools import partial

def test_parquet_writer():
    doc = Dict(key = 'a', data = dictable(a = [pd.Series([1,2,3]), pd.Series([4,5,6])], b = [1,2]), other = Dict(df = pd.DataFrame(dict(a=[1,2,3], b= [4,5,6]))))
    t = mongo_table('test', 'test')
    t.drop()
    p = mongo_table('test', 'test', writer = 'c:/temp/%key.parquet')
    p.writer
    _ = p.insert_one(doc)
    assert eq(p[0]['data'], doc['data'])
    assert eq(t[0]['other'], doc['other'])
    d = t.read(0, passthru)    
    assert d['data']['a'][0]['path'] == 'c:/temp/a/data/a/0.parquet'
    assert d['other']['df']['path'] == 'c:/temp/a/other/df.parquet'
    p.drop()
    assert len(p) == 0
    
def test_csv_writer():
    doc = Dict(key = 'a', data = dictable(a = [pd.Series([1,2,3]), pd.Series([4,5,6])], b = [1,2]), other = Dict(df = pd.DataFrame(dict(a=[1,2,3], b= [4,5,6]))))
    t = mongo_table('test', 'test')
    t.drop()
    p = mongo_table('test', 'test', writer = 'c:/temp/%key.csv')
    p.writer
    _ = p.insert_one(doc)
    assert eq(p[0]['data'], doc['data'])
    assert eq(t[0]['other'], doc['other'])
    d = t.read(0, passthru)    
    assert d['data']['a'][0]['path'] == 'c:/temp/a/data/a/0.csv'
    assert d['other']['df']['path'] == 'c:/temp/a/other/df.csv'
    p.drop()
    assert len(p) == 0


def test_root_path():
    root = 'c:/%school/%pupil.name/%pupil.surname/' 
    doc = dict(school = 'kings',  
               pupil = dict(name = 'yoav', surname = 'git'),  
               grades = dict(maths = 100, physics = 20, chemistry = 80),  
               report = dict(date = dt(2000,1,1),  
                             teacher = dict(name = 'adam', surname = 'cohen') 
                             ) 

                ) 

    assert root_path(doc, root) == 'c:/kings/yoav/git/' 
    root = 'c:/%school/%pupil.name_%pupil.surname/' 
    assert root_path(doc, root) == 'c:/kings/yoav_git/' 
    root = 'c:/archive/%report.date/%pupil.name.%pupil.surname/' 
    assert root_path(doc, root, '%Y') == 'c:/archive/2000/yoav.git/'  # can choose to format dates by providing a fmt. 


    root = 'c:/%name/%surname/%age/'
    doc = dict(name = 'yoav', surname = 'git')
    with pytest.raises(ValueError):
        root_path(doc, root)

def test_parquet_encode():
    value = dict(a = 1)
    path = 'c:/.parquet'
    assert parquet_encode(value, path) == value
    assert csv_encode(value, path) == value

def test_parquet_write():
    s = pd.Series([1,2,3], drange(2))
    db_with_root = partial(mongo_table, db = 'test', table = 'test', writer = 'c:/temp/test.parquet')
    db_no_root = partial(mongo_table, db = 'test', table = 'test', writer = '.parquet')
    c = parquet_write(dict(a = 1, data = s, root = 'c:/temp/test/'))
    assert eq(pd_read_parquet('c:/temp/test/data.parquet'), s)
    c = parquet_write(dict(a = 1, other_data = s, db = db_with_root), root = 'c:/temp/test')
    assert eq(pd_read_parquet('c:/temp/test/other_data.parquet'), s)
    c = parquet_write(dict(a = 1, data = s, db = db_no_root))
    assert eq(c['data'], s)

