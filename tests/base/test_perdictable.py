from pyg import join, perdictable, ewma, dictable, dt, getargspec, drange, eq, dt_bump, div_, mul_, add_, sub_, pow_
import pandas as pd; import numpy as np
import pytest



def test_join():
    assert join(dict(a = 1, b = 2, c = 3), on = []) == dictable(a = 1, b = 2, c = 3)
    assert join(dict(a = 1, b = 2, c = 3), on = ['some key']) == dictable(a = 1, b = 2, c = 3)
    assert join(inputs = dict(a = dictable(a = [1,2,3]), b = 2, c = 3), on = None, defaults = None) == dictable(a = [1,2,3], b = 2, c = 3)
    

    inputs = dict(x = dictable(a = [1,2], data = [2,3])); on= 'a'
    assert join(inputs, on) == dictable(a = [1,2], x = [2,3])
    inputs = dict(x = dictable(a = [1,2], z = [2,3])); on= 'a'
    assert join(inputs, on) == dictable(a = [1,2], x = [2,3])
    inputs = dict(x = dictable(a = [1,2], other_column = [4,5], data = [2,3])); on= 'a'
    assert join(inputs, on) == dictable(a = [1,2], x = [2,3])
    inputs = dict(x = dictable(a = [1,2], b = [4,5], c = [2,3])); on= 'a'
    with pytest.raises(KeyError):
        join(inputs, on)
    inputs = dict(x = dictable(a = [1,2], b = [4,5], c = [2,3])); on= 'a'
    assert join(inputs, on, 'c') == dictable(a = [1,2], x = [2,3]) #not sure if want this
    assert join(inputs, on, ['c']) == dictable(a = [1,2], x = [2,3]) #not sure if want this
    assert join(inputs, on, dict(x = 'c')) == dictable(a = [1,2], x = [2,3])

    
def test_join_no_on():
    inputs = dict(x = dictable(a = [1,2], data = [2,3])); on= None
    assert join(inputs, on) == dictable(x = [2,3])
    inputs = dict(x = dictable(a = [1,2], data = [2,3])); on= []
    assert join(inputs, on) == dictable(x = [2,3])
    inputs = dict(x = dictable(a = [1,2], z = [2,3]))
    with pytest.raises(KeyError):
        join(inputs, on)
    inputs = dict(x = dictable(a = [1,2], other_column = [4,5], data = [2,3]))
    assert join(inputs, on) == dictable(x = [2,3])
    inputs = dict(x = dictable(a = [1,2], b = [4,5], c = [2,3]))
    with pytest.raises(KeyError):
        join(inputs, on)
    inputs = dict(x = dictable(a = [1,2], b = [4,5], c = [2,3]))
    assert join(inputs, on, 'c') == dictable(x = [2,3]) #not sure if want this
    assert join(inputs, on, ['c']) == dictable(x = [2,3]) #not sure if want this
    assert join(inputs, on, dict(x = 'c')) == dictable(x = [2,3])


def test_join_with_defaults():
    x = dictable(a = [1,2,4], x = [1,2,4])
    y = dictable(a = [1,2,3], x = [5,6,7])
    on = 'a'
    assert join(dict(x = x, y = y), on = on) == dictable(a = [1,2,], x = [1,2], y = [5,6])
    assert join(dict(x = x, y = y), on = 'a', renames = 'x') == dictable(a = [1,2,], x = [1,2], y = [5,6])
    assert join(dict(x = x, y = y), on = 'a', defaults = dict(x = None)) == dictable(a = [1,2,3], x = [1,2,None], y = [5,6,7])
    assert join(dict(x = x, y = y), on = 'a', defaults = dict(y = None)) == dictable(a = [1,2,4], x = [1,2,4], y = [5,6,None])
    assert join(dict(x = x, y = y), on = 'a', defaults = dict(x = None, y = None)) == dictable(a = [1,2,3,4], x = [1,2,None,4], y = [5,6,7,None])

def test_join_with_no_on():
    assert join(dict(x = 1, y = dictable(y = [1,2,3])), on = None) == dictable(y = [1,2,3], x = 1)
    assert join(dict(x = dictable(x = [1,2]), y = dictable(y = [1,2])), on = None) == dictable(x = [1,1,2,2], y = [1,2,1,2])
    assert join(dict(x = 1, y = dictable(y = [1,2,3])), on = []) == dictable(y = [1,2,3], x = 1)
    assert join(dict(x = dictable(x = [1,2]), y = dictable(y = [1,2])), on = []) == dictable(x = [1,1,2,2], y = [1,2,1,2])
    
    

def test_perdictable_single_value_no_expiry():
    x = dictable(a = ['a', 'b'], x = [1,2])
    y = dictable(a = ['a', 'b'], y = [1,2])
    f = lambda x, y: x + y
    p = perdictable(f, 'a')
    assert p(x = 1, y = 2) == 3
    assert p(x = x, y = y) == dictable(a = ['a', 'b'], data = [2,4])
    p = perdictable(f, 'a', col = 'col')
    assert p(x = x, y = y) == dictable(a = ['a', 'b'], col = [2,4])
    f = lambda x, y, data = 0: x + y + data
    p = perdictable(f, 'a', output_is_input=True)
    data = p(x = x, y = y)    
    assert p(x = x, y = y, data = data) == dictable(a = ['a', 'b'], data = [4,8])
    p = perdictable(f, 'a', output_is_input=False)
    assert p(x = x, y = y, data = data) == dictable(a = ['a', 'b'], data = [2,4]) # data is excluded from the calculation
    p = perdictable(f, 'a', output_is_input='data')
    assert p(x = x, y = y, data = data) == dictable(a = ['a', 'b'], data = [4,8])
    p = perdictable(f, 'a', output_is_input='something_else')
    assert p(x = x, y = y, data = data) == dictable(a = ['a', 'b'], data = [2,4])


def test_perdictable_single_value_with_expiry():
    x = dictable(a = ['a', 'b'], x = [1,2])
    y = dictable(a = ['a', 'b'], y = [1,2])
    data = dictable(a = ['a', 'b'], data = [None, None])
    expiry = dictable(a = ['a', 'b'], expiry = [dt(-10),dt(10)])
    f = lambda x, y: x + y
    p = perdictable(f, 'a', if_none = False)
    assert p(expiry = expiry, x = x, y = y) == dictable(a = ['a', 'b'], data = [2,4])
    assert p(expiry = expiry, x = x, y = y, data = data) == dictable(a = ['a', 'b'], data = [None,4]) # do not recalculate past

    p = perdictable(f, 'a', if_none = True)
    assert p(expiry = expiry, x = x, y = y, data = data) == dictable(a = ['a', 'b'], data = [2,4]) # do not recalculate past
    data = dictable(a = ['a', 'b'], data = ['random', 'result'])
    assert p(expiry = expiry, x = x, y = y, data = data) == dictable(a = ['a', 'b'], data = ['random',4]) # do not recalculate past


def test_perdictable_dict_value_no_expiry():
    x = dictable(a = ['a', 'b'], x = [1,2])
    y = dictable(a = ['a', 'b'], y = [1,2])
    f = lambda x, y: dict(sum = x + y, prod = x*y); f.output = ['sum', 'prod']
    p = perdictable(f, 'a')
    assert p(x = 1, y = 2) == dict(sum = 3, prod = 2)
    assert p(x = x, y = y) == dict(sum = dictable(a = ['a', 'b'], sum = [2,4]), 
                                   prod = dictable(a = ['a', 'b'], prod = [1,4]))
    f = lambda x, y, sum = 0, prod = 1: dict(sum = x + y + sum, prod = x * y * prod); f.output = ['sum', 'prod']
    p = perdictable(f, 'a', output_is_input=True)
    cache = p(x = x, y = y)    
    assert p(x = x, y = y, **cache) ==dict(sum = dictable(a = ['a', 'b'], sum = [4,8]), 
                                      prod = dictable(a = ['a', 'b'], prod = [1,16]))
    p = perdictable(f, 'a', output_is_input=False)
    assert p(x = x, y = y, **cache) == dict(sum = dictable(a = ['a', 'b'], sum = [2,4]), prod = dictable(a = ['a', 'b'], prod = [1,4]))
    # data is excluded from the calculation
    p = perdictable(f, 'a', output_is_input='sum') ## only sum is used in the input calculations
    assert p(x = x, y = y, **cache) == dict(sum = dictable(a = ['a', 'b'], sum = [4,8]), 
                                            prod = dictable(a = ['a', 'b'], prod = [1,4]))



def test_perdictable_dict_value_with_expiry():
    x = dictable(a = ['a', 'b'], x = [1,2])
    y = dictable(a = ['a', 'b'], y = [1,2])
    f = lambda x, y: dict(sum = x + y, prod = x*y); f.output = ['sum', 'prod']
    expiry = dictable(a = ['a', 'b'], expiry = [dt(-10),dt(10)])
    cache = dict(sum = dictable(a = ['a', 'b'], sum = [None, None]), prod = dictable(a = ['a', 'b'], prod= [None, None]))
    p = perdictable(f, 'a', if_none = False)
    
    assert p(expiry = expiry, x = x, y = y) == dict(sum = dictable(a = ['a', 'b'], sum = [2,4]), prod = dictable(a = ['a', 'b'], prod = [1,4]))
    assert p(expiry = expiry, x = x, y = y, **cache) == dict(sum = dictable(a = ['a', 'b'], sum = [None,4]), prod = dictable(a = ['a', 'b'], prod = [None,4]))

    p = perdictable(f, 'a', if_none = True)
    assert p(expiry = expiry, x = x, y = y, **cache) == dict(sum = dictable(a = ['a', 'b'], sum = [2,4]), prod = dictable(a = ['a', 'b'], prod = [1,4]))



def test_perdictable_argspec():
    f = lambda a, b: a+b
    assert getargspec(perdictable(f)).args == ['a', 'b', 'expiry', 'data'] and getargspec(perdictable(f)).defaults == (None, None)


def test_perdictable_cache():
    oil = dictable(y = dt().year-1, m = range(3, 13, 3)) + dictable(y = dt().year, m = range(3, 13, 3))
    oil = oil(ticker = lambda y, m: 'OIL_%i_%s'%(y, m if m>9 else '0%i'%m))
    expiry = oil(expiry = lambda y, m: dt(y,m+1,1)) # note that dt(y,13,1) is fine and rolls over to (y+1,1,1)

    def fake_ts(ticker, expiry):
        return pd.Series(np.random.normal(0,1,100), drange(dt_bump(expiry,-99), expiry)).cumsum()

    on =  ['y', 'm', 'ticker']
    self = perdictable(fake_ts, on = on)

    inputs = dict(ticker = oil)
    data = self(expiry = expiry, **inputs)
    new_data = self(expiry = expiry, data = data, **inputs)
    assert eq(new_data[0].data,data[0].data)
    assert not eq(new_data[-1].data,data[-1].data)
    

def test_operators():
    assert div_(3,4) == 0.75    
    assert add_(3,4) == 7    
    assert mul_(3,4) == 12  
    assert sub_(3,4) == -1    
    assert pow_(3,4) == 81


# if False:
#     from pyg import *
#     oil = dictable(y = dt().year-1, m = range(3, 13, 3)) + dictable(y = dt().year, m = range(3, 13, 3))
#     oil = oil(ticker = lambda y, m: 'OIL_%i_%s'%(y, m if m>9 else '0%i'%m))
#     expiry = oil(expiry = lambda y, m: dt(y,m+1,1)) # note that dt(y,13,1) is fine and rolls over to (y+1,1,1)

#     def fake_ts(ticker, expiry):
#         return pd.Series(np.random.normal(0,1,100), drange(dt_bump(expiry,-99), expiry)).cumsum()

#     def fake_tss(ticker, expiry):
#         return dict(a = fake_ts(ticker, expiry), b = fake_ts(ticker, expiry))
#     fake_tss.output = ['a', 'b']

#     on =  ['y', 'm', 'ticker']
#     self = perdictable(fake_ts, on = ['y', 'm', 'ticker'])
#     inputs = dict(ticker = oil)
#     prices = self(expiry = expiry, **inputs)

#     rtns = perdictable(diff, on = on)(a = prices, expiry = expiry)
#     vol = perdictable(ewma, on = on)(a = rtns, n = 30, expiry = expiry)
#     self = perdictable(div_, on = on)
    
#     inputs = dict(a = rtns, b = vol)



#     rtns = perdictable(ewma_, on = ['y', 'm', 'ticker'], output_is_input = False)
#     inputs = dict(a = data, n = 10)
#     datastate = self(a = data, n = 10)

    
#     svgs = perdictable(ewma_, on = ['y', 'm', 'ticker'], output_is_input = False)
#     inputs = dict(ticker = oil, data = data)
#     new_data = self(expiry = expiry, **inputs)
#     data[0], new_data[0]
#     data[-1], new_data[-1]

#     self = perdictable(fake_tss, on = ['y', 'm', 'ticker'])
#     data = self(ticker = oil)
#     inputs = dict(ticker = oil, **data)
#     new_data = self(expiry = expiry, **inputs)
#     assert eq(data['a'][0], new_data['a'][0])
#     assert eq(data['b'][0], new_data['b'][0])

#     data['a'][-1], new_data['a'][-1]


#     self = cell(perdictable(fake_ts, on = ['y', 'm', 'ticker']), ticker = oil, expiry = expiry)()
#     getargspec(self)
#     self.data
#     self = self.go(1)    
#     self.data
#     new = prices.go(1)
    
#     self = perdictable(diff, on = ['y', 'm', 'ticker']); inputs = dict(a = prices)
#     rtns = perdictable(diff, on = ['y', 'm', 'ticker'])(a = prices, expiry = expiries)
#     vols = perdictable(ewmstd, on = ['y', 'm', 'ticker'])(a = rtns, n = 30, expiry = expiries)
#     normalized_rtns = perdictable(div_, on = ['y', 'm', 'ticker'])(a = rtns, b = vols, expiry = expiries)



    

