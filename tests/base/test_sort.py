from pyg import sort, cmp, Cmp, dictattr, dt, drange, is_nan, dictable, eq
import numpy as np; import pandas as pd
import datetime


def test_cmp():
    t =  datetime.datetime(2021, 2, 20, 22, 38, 12)
    values = [None, np.nan, np.inf, -np.inf, 3.2, 1, t, datetime.timedelta(1), datetime.timedelta(2), dict(a = 1, b = 2), 
              dict(a = 3,b=4,c = 5), dictattr(a = 3, b = 4, c=5), pd.Series(np.arange(4)), True, False, np.str_('hello'), 
              'hello', 'hell', 'jello', np.arange(10), np.arange(5,8), drange(5), [0, 1,2,3], [0,1,3], range(3)]
     
    sorted_values = sorted(values, key = Cmp)

    for i in range(len(values)):
        for j in range(i):
            jth, ith = sorted_values[j], sorted_values[i]
            if not (is_nan(jth) and is_nan(ith)):
                assert cmp(jth, ith) == -1
                assert cmp(ith, jth) == 1

    
    assert sorted([1,2,0,4,2,4,7,4,5,2,4], key = Cmp) == sorted([1,2,0,4,2,4,7,4,5,2,4])
    unsorted = list('the quick brown fox jumped over the fence today')
    assert sorted(unsorted) == sorted(unsorted, key = Cmp)

    assert cmp('2', 2) == 1
    assert cmp(np.int64(2), 2) == 0
    assert cmp(None, 2.0) == -1 # None is smallest
    assert cmp([1,2,3], [4,5]) == 1 # [1,2,3] is longer
    assert cmp([1,2,3], [1,2,0]) == 1 # lexical sorting 
    assert cmp(dict(a = 1, b = 2), dict(a = 1, c = 2)) == -1 # lexical sorting on keys
    assert cmp(dict(a = 1, b = 2), dict(b = 2, a = 1)) == 0 # order does not matter


    assert Cmp('2')> 2
    assert Cmp(None)<2.0 # None is smallest
    assert Cmp([1,2,3])> [4,5] # [1,2,3] is longer
    assert Cmp([1,2,3])> [1,2,0] # lexical sorting 
    assert Cmp(dict(a = 1, b = 2))< dict(a = 1, c = 2)

    assert Cmp([1,2,3,4, np.nan]) > [1,2,3,4]
    assert Cmp(np.nan)<1


def test_sort():
    t =  datetime.datetime(2021, 2, 20, 22, 38, 12)
    values = [None, np.nan, np.inf, -np.inf, 3.2, 1, t, datetime.timedelta(1), datetime.timedelta(2), dict(a = 1, b = 2), 
              dict(a = 3,b=4,c = 5), dictattr(a = 3, b = 4, c=5), pd.Series(np.arange(4)), True, False, np.str_('hello'), 
              'hello', 'hell', 'jello', np.arange(10), np.arange(5,8), drange(5), [0, 1,2,3], [0,1,3], range(3)]
    assert eq(sort(values), sorted(values, key = Cmp))
