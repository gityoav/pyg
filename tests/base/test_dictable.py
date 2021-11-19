from pyg import dictable, read_csv, Dict, last, dictattr, alphabet, drange, dict_concat, dt, encode, decode, mongo_table, is_tuple
import pandas as pd
import numpy as np
import pytest
import tempfile
import os
import re

@pytest.mark.skip(reason = 'file not there')
def test_dictable_init_Excel():
    fname = 'd:/dropbox/Yoav/python/pyg/tests/base/book.xlsx'
    df = pd.ExcelFile(fname).parse()
    rs = dictable(fname)
    assert dictable(df) == rs



def test_dictable_init():
    assert dict(dictable(data = [1,2,3], b = 1)) == dict(data = [1,2,3], b = [1,1,1])
    assert dict(dictable(a = 1, b = 2)) == dict(a = [1], b = [2])
    assert dict(dictable([[1,2,3]], ['a', 'b', 'c'])) == {'a': [1], 'b': [2], 'c': [3]}
    assert dict(dictable([[1,2,3]], ['a', 'b', 'c'])) == {'a': [1], 'b': [2], 'c': [3]}
    assert dict(dictable([dict(a=1, b=2), dict(a=2,b=3)])) == {'a': [1, 2], 'b': [2, 3]}
    assert dict(dictable([1,2,3])) == dict(data = [1,2,3])
    assert dict(dictable(pd.DataFrame(dict(a=[1,2], b= 3)))) == {'a': [1, 2], 'b': [3, 3]}
    assert dict(dictable(pd.DataFrame(dict(a=[1,2], b= 3)).set_index('a'))) == {'a': [1, 2], 'b': [3, 3]}
    assert dict(dictable([['a', 'b'], [1,2], [3,4]])) == dict(a = [1,3], b = [2,4])
    
    z = dict(a = [1,2,3,4,], b = [4,5,6,7])
    assert dictable(zip(*z.values()), z.keys()) == dictable(z)
    assert dict(dictable(a = [1,2,3], b = None)) == dict(a = [1,2,3], b = [None]*3)
    data = [('a', 1), ('b', 2), ('c', 3)]
    assert dictable(data) == dictable(a = 1, b = 2, c = 3)
    assert dictable([1,2,3], 'a') == dictable(a = [1,2,3])
    assert dict(dictable([1,2,3])) == dict(data = [1,2,3])
    assert dict(dictable(1)) == dict(data = [1])
    assert dict(dictable('string')) == dict(data = ['string'])
    assert dict(dictable(data = [1,2,3])) == dict(data = [1,2,3])


def test_dictable_init_with_columns():
    assert dict(dictable(columns = 'a')) == dict(a = [])
    assert dict(dictable(columns = ['a', 'b'])) == dict(a = [], b = [])
    assert dict(dictable([], columns = ['a', 'b'])) == dict(a = [], b = [])
    assert dict(dictable(dict(a = 1, b = 2, c = [3,4]), columns = ['a', 'b'])) == dict(a = [1], b = [2])
    assert dict(dictable(dict(a = 1, b = [2,5,6], c = [3,4]), columns = ['a', 'b'])) == dict(a = [1,1,1,], b = [2,5,6])
    assert dict(dictable(dict(a = 1, b = [2,5,6], c = [3,4]), columns = ['a', 'c'])) == dict(a = [1,1], c = [3,4])
    with pytest.raises(ValueError):
        dict(dictable(dict(a = 1, b = [2,5,6], c = [3,4]), columns = ['a', 'b', 'c']))
    with pytest.raises(ValueError):
        dict(dictable(a = 1, b = [2,5,6], c = [3,4], columns = ['a', 'b', 'c']))


def test_dictable_init_from_tree():    
    columns = 'students/%id/%attr/%value'
    data = dict(students = dict(james_munro = dict(name = 'james', age = 1, surname = 'munro'), abe_lincoln = dict(name = 'abe', age = 200, surname = 'lincoln')))
    rs = dictable(data, columns).sort('id', 'attr')
    assert dict(rs) == {'id': ['abe_lincoln'] * 3 + [ 'james_munro'] * 3, 
                        'attr': ['age', 'name', 'surname', 'age', 'name', 'surname'],
                        'value': [200, 'abe', 'lincoln', 1, 'james', 'munro']}
    

def test_dictable_get():
    d = dictable(a = [1,2,3,], b = 4)
    assert d.get('a') == d.a == [1,2,3]
    assert d.get('c') == [None, None, None]
    assert d.get('c', 0) == [0,0,0]
    
    

def test_dictable_init_from_cursor():
    t = mongo_table('test', 'test')
    t.drop()
    d = dictable(a = [1,2,3], b = [4,5,6])
    t.insert_many(d)
    c = t[::]
    assert c[['a', 'b']] == d
    c = dictable(t)
    assert c[['a', 'b']] == d
    c = dictable(t.collection)
    assert c[['a', 'b']] == d
    t.drop()

def test_dictable_shape():
    assert dictable().shape == (0,0)
    assert dictable(a = 1).shape == (1,1)
    assert dictable(a = [1,2,3]).shape == (3,1)
    assert dictable(a = [1,2,3]).shape == (3,1)
    assert dictable(a = [1,2,3], b = 4).shape == (3,2)
    
    
def test_dictable_columns():
    assert dictable().columns == []
    assert dictable(a = [1,2,3]).columns == ['a']
    assert dictable(a = [1,2,3], b = 1).columns == ['a', 'b']

def test_dictable_set():
    d = dictable(a = [1,2,3])
    d['b'] = 1
    d['c'] = None
    assert d.b == [1,1,1]
    assert d.c == [None] * 3
    d['e'] = [1,2,3]
    assert d.e == [1,2,3]
    with pytest.raises(ValueError):
        d['f'] = [1,2,3,4]
        
def test_dictable_set_empty():
    d = dictable()    
    d['a'] = [1,2,3]
    assert d.a == [1,2,3]
    d = dictable()    
    d['a'] = None
    assert d.a == [None]
    with pytest.raises(ValueError):
        d['b'] = [1,2,3]


def test_dictable_getitem_zip():
    d = dictable(a = [1,2,3])
    assert d[range(2)] == dictable(a = [1,2])
    d = dictable(a = [1,2,3], b = 4, c = 5)
    assert d[dict(a = 1, b = 2).keys()] == d[['a', 'b']]
    assert d[dict(a = 1, b = 2).values()] == d[[1,2]]
    
def test_dictable_getitem_fail():
    d = dictable(a = [1,2,3])
    with pytest.raises(ValueError):
        d[[dt(0)]]    

    with pytest.raises(KeyError):
        d[dt(0)]


def test_dictable_find():
    d = dictable(a = list(alphabet), b = range(26))
    assert d.find_b(a = 'a') == 0
    d = d + d
    assert d.find_b(a = 'a') == 0 ## multiple entries but that's ok
    d = d + d(b = lambda b: b**2)
    assert d.find_b(a = 'b') == 1 ## multiple entries but that's ok
    with pytest.raises(KeyError):
        d.find_nokey(a = 'c')         
    with pytest.raises(ValueError):
        d.find_b(a = 'c')         
    with pytest.raises(ValueError):
        d.find_b(a = 'nothing') 
        

def test_dictable_repr():
    d = dictable(x = [1,2,3,np.nan], y = [None,4,3,5]) 
    assert d.__repr__() == 'dictable[4 x 2]\nx  |y   \n1  |None\n2  |4   \n3  |3   \nnan|5   '
    assert d.__str__() == 'x  |y   \n1  |None\n2  |4   \n3  |3   \nnan|5   '

def test_dictable_inc():
    d = dictable(a = [1,2,3,4])
    assert d.inc() == d
    assert d.listby() == d    
    assert d.listby([]) == dictable(a = [d.a])

    d = dictable(x = [1,2,3,np.nan], y = [None,4,3,5]) 
    assert d.inc(x = np.nan) == dictable(x = np.nan, y = 5)
    assert d.exc(x = np.nan) == dictable(x = [1,2,3], y = [None, 4, 3])
    assert d.exc(y = None, x = np.nan) == dictable(x = [2,3], y = [4, 3])
    assert d.inc(x = 1) == dictable(x = 1, y = None)             
    assert d.inc(x = [1]) == dictable(x = 1, y = None)             
    assert d.exc(x = [1]) == dictable(x = [2,3,np.nan], y = [4,3,5])             
    assert d.exc(x = 1) == dictable(x = [2,3,np.nan], y = [4,3,5])             
    assert d.inc(y = None) == dictable(x = 1, y = None)             
    assert d.inc(x = [1,2]) == dictable(x = [1,2], y = [None,4])  
    d = dictable(x = [1,2,3,np.nan], y = [0, 4,3,5]) 
    assert d.inc(lambda x,y: x>y) == dictable(x = 1, y = 0) 
    assert d.inc(dict(y = 4)) == dictable(x = 2, y = 4)


def test_dictable_exc():
    d = dictable(a = [1,2,3,4])
    assert d.inc(dict(a = 3)) == dictable(a = 3)
    assert d.exc(dict(a = 3)) == dictable(a = [1,2,4])


def test_dictable_to_string():
    d = dictable(a = [1,2,3,4])
    assert d.to_string(rowsep = 'header') == 'a\n-\n1\n2\n3\n4'


def test_dictable_join_mismatch():
    d = dictable(a = [1,2,3], b = [4,5,6])
    e = dictable(c = [1,2,3])    
    with pytest.raises(ValueError):
        d.join(e, ['a', 'b'], 'c')
    with pytest.raises(ValueError):
        d.xor(e, ['a', 'b'], 'c')
        

def test_dictable_xor_no_rhs():
    d = dictable(a = [1,2,3], b = [4,5,6])
    e = dictable(c = [4,5,6])    
    assert d/e == d

def test_unlist():
    d = dictable(a = [], b = [])
    assert d.listby('a').unlist() == d
    d = dictable(a = [1,2,1,2,3,3], b = [1,2,3,4,5,6])
    assert d.listby('a').unlist() == d.sort('a')
    assert d.listby() == d.sort('a', 'b')
    self = d.listby([])
    self.unlist()
    
    
def test_dictable_listby_empty():
    d = dictable(a = [1,2,3,4])
    assert d.listby() == d    
    assert d.listby([]) == dictable(a = [d.a])
    d = dictable(a = [], b = [])
    assert d.listby('a') == d
 
    
def test_dictable_set_underscore():
    a = dictable(a = [1,2,3])
    a.b = 4
    assert a == dictable(a = [1,2,3], b = 4)
    a._c = 5
    assert '_c' not in a.keys()    
    assert a._c == 5
    assert '_c' not in dir(a)
    assert decode(encode(a)) == a 
    with pytest.raises(AttributeError):
        decode(encode(a))._c    
    del a._c
    with pytest.raises(AttributeError):
        a._c
        
    del a.b
    assert a == dictable(a = [1,2,3])
        
def test_dictable_text_box():
    a = dictable(a = range(26), b = list(alphabet), c = drange(2000,25))
    assert a.to_string() == 'a |b|c                  \n0 |a|2000-01-01 00:00:00\n1 |b|2000-01-02 00:00:00\n2 |c|2000-01-03 00:00:00\n3 |d|2000-01-04 00:00:00\n4 |e|2000-01-05 00:00:00\n5 |f|2000-01-06 00:00:00\n6 |g|2000-01-07 00:00:00\n7 |h|2000-01-08 00:00:00\n8 |i|2000-01-09 00:00:00\n9 |j|2000-01-10 00:00:00\n10|k|2000-01-11 00:00:00\n11|l|2000-01-12 00:00:00\n12|m|2000-01-13 00:00:00\n13|n|2000-01-14 00:00:00\n14|o|2000-01-15 00:00:00\n15|p|2000-01-16 00:00:00\n16|q|2000-01-17 00:00:00\n17|r|2000-01-18 00:00:00\n18|s|2000-01-19 00:00:00\n19|t|2000-01-20 00:00:00\n20|u|2000-01-21 00:00:00\n21|v|2000-01-22 00:00:00\n22|w|2000-01-23 00:00:00\n23|x|2000-01-24 00:00:00\n24|y|2000-01-25 00:00:00\n25|z|2000-01-26 00:00:00'
    

def test_dict_concat():
    a = dict(a = 1, b = 2, c = 3)
    b = dict(a = 1, b = 2, d = 3)
    c = dict(b = 2, c = 5, e = 7)
    assert dict_concat(a,b,c) == { 'a': [1, 1, None], 'b': [2, 2, 2],
                                  'c': [3, None, 5],  'd': [None, 3, None], 'e': [None, None, 7]}

    dicts = [dict(a=1,b=2), dict(a=3,b=4,c=5)]
    assert dict_concat(dicts) == dict(a = [1,3], b = [2,4], c = [None,5])
    dicts = [dict(a=1,b=2)]
    assert dict_concat(dicts) == dict(a = [1], b = [2])
    assert dict_concat([]) == dict()
    assert dictable([]).shape == (0,0)
    assert dictable.concat() == dictable()

def test_dictable_csv():
    d = dictable(a = [1,2,3], b = 4)
    txt = 'a,b\n1,4\n2,4\n3,4\n'    
    with tempfile.NamedTemporaryFile(suffix = '.csv', mode = 'w', delete = False) as f:
        name = f.name
        f.write(txt)
    read = read_csv(name)
    res = dictable(name)
    os.remove(name)    
    assert read == [['a', 'b'], ['1', '4'], ['2', '4'], ['3', '4']]
    assert res.do(int) == d

def test_dictable_from_tree():
    tree = dict(james = dict(age = 1, gender = 'm'), alan = dict(age = 2, gender = 'they'), barbara = dict(age = 3, gender = 'f'))
    res = dictable(tree, '%name/age/%age')
    assert res == dictable(age = [1,2,3], name = ['james', 'alan', 'barbara'])


def test_dictable_filter_all_returns_columns():
    a = dictable(a = [1,2,3])
    assert a[[False, False, False]].keys() == a.keys()
    assert a.inc(lambda a: a>4).keys() == a.keys()
    assert a.exc(lambda a: a<4).keys() == a.keys()
    
    
def test_dictable_inc2():
    self = dictable(a = [1,2,3,3], b = [4,5,6,7])
    assert self.inc(a = 1) == dictable(a = 1, b = 4)
    assert self.inc(a = [1,2]) == dictable(a = [1,2], b = [4,5])
    assert self.inc(a = 3, b = 6) == dictable(a = 3, b = 6)    
    assert self.inc(lambda a: a>2, lambda b: b<7) == dictable(a = 3, b = 6)    
    assert self.inc(lambda a: a>2, b = 7) == dictable(a = 3, b = 7)    
    assert self.inc() is self
    assert self.exc() is self

def test_dictable_getitem():
    self = dictable(a = [1,2,3,3], b = [4,5,6,7])
    assert self['a'] == [1,2,3,3]
    assert self['a', 'b'] == [(1, 4), (2, 5), (3, 6), (3, 7)]
    assert self[0] == Dict(a = 1, b=4)
    assert self[::2] == dictable(a = [1,3], b = [4,6])
    assert self[[True, False, False,False]] == dictable(a = [1], b = [4])
    assert self[[False, False, False,False]] == dictable(a = [], b = [])
    assert self[np.array([False, False, True,False])] == dictable(a = [3], b = [6])
    assert self[[]] == dictable([], ['a', 'b'])

def test_dictable_xyz():
    self = dictable(x = [1,2,3]) * dictable(y = [1,2,3]) 
    with pytest.raises(ValueError):
        self.xyz(lambda x: x+1, 'y', lambda x, y: x*y)
    x = 'x'; y = 'y'; z = lambda x, y: x * y
    assert self.xyz(x,y,z, last) == dictable({'x': [1,2,3], '1': [1,2,3], '2': [2,4,6], '3': [3,6,9]})
    assert self.xyz(x,y,z, last).unpivot(x,y,'z').do(int, 'y') == self(z = z).sort(x,y)

def test_dictable_xyz_func_y():
    self = dictable(x = [1,2,3]) * dictable(y = [1,2,3]) 
    x = 'x'; y = lambda y: 'x%i'%y; z = lambda x, y: x * y
    assert self.xyz(x,y,z, last) == dictable(x = [1,2,3], x1 = [1,2,3], x2 = [2,4,6], x3 = [3,6,9])

def test_dictable_xyz_multix():
    self = dictable(a = 1, b = [2,2,], c = [3,4]) + dictable(a = 2, b = [2,4,], c = [5,6]) + dictable(a = 3, b = [0,2,], c = [1,8])
    res = self.xyz(['a', 'b'], 'c', lambda a,b,c: a+b+c, last)    
    assert res.unpivot(['a', 'b'], 'c', 'd').exc(d = None).do(int, 'c') == self(d = lambda a,b,c: a+b+c)


def test_dictable_join():
    self = dictable(a = [1,2,3,4,5])
    for tp in (dict, dictable, Dict):
        other = tp(a = [1,2,3,6,7])
        assert self * other == dictable(a = [1,2,3])
        assert self / other == dictable(a = [4,5])
    assert dictable(other)/self == dictable(a = [6,7])

def test_dictable_join2():
    a = dictable(a = [1,2,3,4], b  = 4)
    b = dictable(a = [1,2,3,4], c = [5,6,7,8])
    with pytest.raises(ValueError):
        a.join(b,'a', ['a','c'])

    assert a.join(b, lambda a: a+1, 'a') == dictable(a = [2,3,4], b = 4, c = [6,7,8])

    with pytest.raises(ValueError):
        a.join(b,lambda a: a+1, lambda a: a+1)

    assert a.join(b, lambda a: -a, 'a') == dictable(a=[],b=[], c=[])

def test_tictable_join_mode():
    a = dictable(a = [1,2,3,4], b = 4)
    b = dictable(a = [1,2,3,4], b = 5)
    assert a*b == dictable(a = [], b = [])
    assert a.join(b, 'a') == dictable(a = [1,2,3,4], b = [(4,5)])
    assert a.join(b, 'a', mode = 'left') == dictable(a = [1,2,3,4], b = 4)
    assert a.join(b, 'a', mode = 'right') == dictable(a = [1,2,3,4], b = 5)
    assert a.join(b, 'a', mode = lambda x,y: x+y) == dictable(a = [1,2,3,4], b = 9)
    

def test_dictable_join_different_col_names():
    self = dictable(a = [1,2,3,4,5])
    other = dictable(b = [1,2,3,6,7])
    assert self.join(other, 'a', 'b')  == dictable(a = [1,2,3], b = [1,2,3])
    assert self.xor(other, 'a', 'b') == dictable(a = [4,5])
    assert other.xor(self , 'b', 'a') == dictable(b = [6,7])

def test_dictable_join_function():
    self = dictable(a = [1,2,3,4,5])
    other = dictable(b = [1,2,3,6,7])
    assert self.join(other, 'a', lambda b: b-2)  == dictable(a = [1,4,5], b = [3,6,7])
    assert self.xor(other, 'a', lambda b: b-2) == dictable(a = [2,3])
    assert other.xor(self , 'b', lambda a: a+2) == dictable(b = [1,2])


def test_dictable_sort():
    a = dictable(a = [1,2,0,2], b = [4,3,2,1])
    assert a.sort() == a
    assert a.sort([]) == a    
    assert a.sort('a') == dictable(a = [0,1,2,2], b = [2,4,3,1])
    assert a.sort(lambda a: -a) == dictable(a = [2,2,1,0], b = [3,1,4,2])
    assert a.sort('a','b') == dictable(a = [0,1,2,2], b = [2,4,1,3])
    
def test_dictable_to_string():
    d = dictable(a = range(6))
    assert d.to_string() == 'a\n0\n1\n2\n3\n4\n5'
    assert d.to_string(rowsep = '-') == 'a\n-\n0\n-\n1\n-\n2\n-\n3\n-\n4\n-\n5\n-'
    d = dictable(a = range(20))
    assert d.to_string() == 'a \n0 \n1 \n2 \n3 \n4 \n5 \n6 \n7 \n8 \n9 \n10\n11\n12\n13\n14\n15\n16\n17\n18\n19'
    assert d.to_string(cat = 2) == 'a \n0 \n1 \n...\n18\n19'
    assert d.to_string(cat = 3, header = '*', rowsep = '-', footer = '=') == '*\na \n--\n0 \n--\n1 \n--\n2 \n--\n...\n17\n--\n18\n--\n19\n--\n='

    
def test_dictable_groupby():
    d = dictable(a = [1,2,1,2,3,3], b = 4)
    a = d.groupby('a')
    assert a.a == [1,2,3]
    assert a.grp[0] == dictable(b = [4,4])
    assert a.ungroup() == d.sort('a')
    with pytest.raises(ValueError):    
        d.groupby([])
    with pytest.raises(ValueError):    
        d.groupby()
    d = dictable(a = [])
    assert d.groupby() == d
    
def test_dictable_xor():
    a = dictable(a = range(10))
    b = dictable(a = range(3, 8))
    assert a/b == dictable(a = [0,1,2,8,9])
    assert b.xor(a, mode = 'r') == a/b

def test_regex_inc_exc():
    d = dictable(a = [1,2,'hello','world', 'hello kitty'])     
    assert d.inc(a = re.compile('he')) == dictable(a = ['hello', 'hello kitty'])
    assert d.exc(a = re.compile('he')) == dictable(a = [1, 2, 'world'])


def test_tictable_unpivot():
    rs = dictable(name = ['a', 'b', 'c'], maths = [1,2,3], biology = [4,5,6], french = [7,8,9])
    full = rs.unpivot('name', 'subject', 'score')
    science = rs.unpivot('name', dict(science = ['maths', 'biology']), 'score')
    assert science.relabel(science = 'subject') == full.exc(subject = 'french')
