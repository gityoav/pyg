# from pyg import df_update, getitem, reindex, sync, dictable, eq, Dict, joint_index, joint_columns, drange, dt, presync
# import pandas as pd
# import numpy as np

# def test_update():
#     # df = dict(a = 1, b = 2)
#     # kwargs = dict(c=3); updates = None
#     assert df_update(dict(a = 1, b = 2), c = 3, d = lambda a, b: a+b) == dict(a = 1, b = 2, c=3, d = 3)
#     df = pd.Series(dict(a = 1, b= 2))
#     assert eq(df_update(df, c = 3, d = lambda a, b: a+b) , pd.Series(dict(a = 1, b = 2, c=3, d = 3)))
#     assert df_update(dictable(a = [1,2], b= [3,4]), d = lambda a, b: a+b) == dictable(a = [1,2], b = [3,4], d = [4,6])
#     df = pd.DataFrame(dict(a = [1,2], b= [3,4]))
#     assert eq(df_update(df, d = lambda a, b: a+b), pd.DataFrame(dict(a = [1,2], b= [3,4], d = [4,6])))
#     assert eq(df_update(df, d = 0),  pd.DataFrame(dict(a = [1,2], b= [3,4], d = [0,0])))
    
    
# def test_getitem():
#     df = dict(a = 1, b= 2)
#     assert getitem(df, lambda a, b: a+b) == 3
#     assert getitem(df, 'a') == 1
#     assert getitem(df, ['a','b']) == dict(a=1, b=2)
#     assert getitem(df, ('a','b', 'a')) == [1,2,1]
#     assert getitem(df, ('a','b', lambda a, b: a+b)) == [1,2,3]
    

#     df = pd.Series(dict(a = 1, b= 2))
#     assert getitem(df, lambda a, b: a+b) == 3
#     assert getitem(df, 'a') == 1
#     assert getitem(df, ['a','b']) == dict(a=1, b=2)
#     assert getitem(df, ('a','b', 'a')) == [1,2,1]
#     assert getitem(df, ('a','b', lambda a, b: a+b)) == [1,2,3]

#     df = pd.DataFrame(dict(a = [1,2], b= [3,4]))
#     assert eq(getitem(df, lambda a, b: a+b), pd.Series([4,6]))
#     assert eq(getitem(df, 'a'), df.a)
#     assert eq(getitem(df, ['a','b']) , Dict(a=df.a, b=df.b))
#     assert eq(getitem(df, ('a','b')) , [df.a, df.b])

# def test_joint_columns():
#     a = dict(a=1,b=2,c=3)
#     b = pd.Series(dict(a=1, b=2, d = 4))
#     c = pd.DataFrame(dict(a = [1,2,3], e = [4,5,6]))
#     ts = pd.Series([1,2,3], drange(2))
#     assert joint_columns([a,b,c,1,ts], 'ij') == ['a']
#     assert joint_columns([a,b,c,1,ts]) == ['a']
#     assert sorted(joint_columns([a,b,c,1,ts], 'oj')) == ['a', 'b', 'c', 'd', 'e']
#     assert joint_columns([a,b,c,1,ts], 'left') == ['a', 'b', 'c']
#     assert joint_columns([a,b,c,1,ts], 'right') == ['a', 'e']
    
# def test_joint_index():
#     a = pd.Series(range(3), drange(2000,2))
#     b = pd.Series(range(3), drange(2000,4,2))
#     c = pd.Series(range(2), drange(2000,1))
#     assert eq(joint_index([a,b,c,5]) , pd.Index([dt(2000)]))
#     assert eq(joint_index([a,b,c,4], 'ij') , pd.Index([dt(2000)]))
#     assert eq(joint_index([a,b,c,3], 'oj') , pd.Index([dt('2000-01-01'), dt('2000-01-02'), dt('2000-01-03'), dt('2000-01-05')]))
#     assert eq(joint_index([a,b,c,2], 'lj') , pd.Index(drange(2000,2)))
#     assert eq(joint_index([a,b,c,1], 'rj') , pd.Index(drange(2000,1)))

#     assert joint_index([a.values, b.values,c.values]) == 2
#     assert joint_index([a.values, b.values,c.values], 'ij') == 2
#     assert joint_index([a.values, b.values,c.values], 'oj') == 3
#     assert joint_index([a.values, b.values,c.values], 'lj') == 3
#     assert joint_index([a.values, b.values,c.values], 'rj') == 2


# def test_sync():
#     a = pd.Series([0, np.nan, 2], drange(2000,2))
#     b = pd.Series(range(3), drange(2000,4,2))
#     c = pd.Series(range(2), drange(2000,1))
#     df1 = pd.DataFrame(dict(a=a, b=a))
#     df2 = pd.DataFrame(dict(a=a, c=a))
#     s = pd.Series([1,2,3,4], list('abcd'))
#     assert eq(sync([a,b,c]), [v.iloc[:1] for v in [a,b,c]])
#     assert eq(sync([a,b,c], 'ij'), [v.iloc[:1] for v in [a,b,c]])
#     idx = pd.Index([dt('2000-01-01'), dt('2000-01-02'), dt('2000-01-03'), dt('2000-01-05')])
#     assert eq(sync([a,b,c], 'oj'), [pd.Series([0,np.nan,2,np.nan], idx), pd.Series([0,np.nan,1,2], idx), pd.Series([0,1, np.nan,np.nan], idx)])
#     idx = drange(2000,2)
#     assert eq(sync(dfs = [a,b,c, 'something'], how = 'lj'), [pd.Series([0,np.nan,2], idx), pd.Series([0,np.nan,1], idx), pd.Series([0,1, np.nan], idx), 'something'])
#     idx = pd.Index([dt('2000-01-01'), dt('2000-01-02'), dt('2000-01-03'), dt('2000-01-05')])
#     assert eq(sync([a,b,c], 'oj', 'ffill'), [pd.Series([0,0.,2,2.], idx), pd.Series([0,0.,1,2], idx), pd.Series([0,1, 1.,1.], idx)])
#     assert eq(sync(a, 'oj', 'f'), pd.Series([0, 0., 2], drange(2000,2)))
#     assert eq(sync([df1,df2,s]), [df1[['a']], df2[['a']], s[['a']]])

# def test_sync_single_cols():
#     a = pd.Series([1,2,3], drange(2))
#     b = pd.DataFrame(dict(b=[1,2,3]), drange(2))
#     c = pd.DataFrame(dict(c=[1,2,3]), drange(2))
#     d = pd.DataFrame(dict(d=[1,2,3], e = [4,5,6]), drange(2))
#     assert eq(sync([a,b,c]) , [a,b,c])
#     assert eq(sync([a,b,c,d]) , [a,b,c,d])
    

# def test_sync_different_columns():
#     a = dict(a = 1, b = 2)
#     b = dict(a = 3, c = 4)
#     c = dict(a = 5, d = 6)
#     assert sync([a,b,c]) == [dict(a = 1), dict(a = 3), dict(a = 5)]
#     assert sync([a,b,c], chow = 'ij') == [dict(a = 1), dict(a = 3), dict(a = 5)]
#     assert sync([a,b,c], chow = 'oj', default = None) == [{'b': 2, 'c': None, 'a': 1, 'd': None},
#                                                           {'b': None, 'c': 4, 'a': 3, 'd': None},
#                                                           {'b': None, 'c': None, 'a': 5, 'd': 6}]

#     assert sync([a,b,c], chow = 'oj', default = 0) == [{'b': 2, 'c': 0, 'a': 1, 'd': 0},
#                                                           {'b': 0, 'c': 4, 'a': 3, 'd': 0},
#                                                           {'b': 0, 'c': 0, 'a': 5, 'd': 6}]

#     assert sync([a,b,c], chow = 'lj', default = 0) == [a, {'a': 3, 'b': 0}, {'a': 5, 'b': 0}]
#     assert sync([a,b,c], chow = 'rj', default = 0) == [{'a': 1, 'd': 0}, {'a': 3, 'd': 0}, c]
#     assert sync([a,b,c, 5], chow = 'rj', default = 0) == [{'a': 1, 'd': 0}, {'a': 3, 'd': 0}, c, 5]

#     b = pd.Series(b)
#     assert eq(sync([a,b,c], chow = 'lj', default = 0) , [a,  pd.Series({'a': 3, 'b': 0}), {'a': 5, 'b': 0}])

#     c = pd.DataFrame(dict(a = [5,6], d = [7,8]), index = drange(1))
#     assert eq(sync(dfs = [a,b,c], chow = 'ij', default = 0) , [dict(a=1),  pd.Series({'a': 3}), pd.DataFrame(dict(a = [5,6]), index = drange(1))])
#     res = sync(dfs = [a,b,c], chow = 'oj', default = 0)
#     expected =  [df_update(a, c=0, d=0),  df_update(b,b=0,d=0), df_update(c, b=0, c = 0)]    
#     assert eq(getitem(res, ['a','b','c','d']),getitem(expected, ['a','b','c','d']))


