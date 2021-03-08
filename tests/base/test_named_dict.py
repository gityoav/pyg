from pyg import named_dict, eq
import pytest
import pandas as pd; import numpy as np
import datetime
import json

def test_named_dict():
    Customer = named_dict('Customer', ['name', 'date', 'balance'])
    james = Customer('james', 'today', 10) ## construction with parameter order for a dict 
    assert james['balance'] == 10
    assert james.date == 'today'

    bob = Customer('Bob', date = 'yesterday', balance = 20)
    james2 = Customer('james', date = 'today', balance = 10)
    assert james == james2
    
    assert pd.Series(james).date == 'today'
    assert eq(pd.DataFrame([james, bob]).date.values, np.array(['today','yesterday']))

    with pytest.raises(ValueError):
        Customer('bob needs to provide more than just his name')

def test_named_dict_with_defaults():
    Customer = named_dict('Customer', ['name', 'date', 'balance'], defaults = dict(balance = 0))
    james = Customer('james', 'today')
    assert james['balance'] == 0
    with pytest.raises(ValueError):
        named_dict('Customer', ['name', 'date', 'balance'], defaults = dict(date = 0))



def test_named_dict_with_types_checking():
    Customer = named_dict('Customer', ['name', 'date', 'balance'], defaults = dict(balance = 0), types = dict(date = 'datetime.datetime', balance = 'pyg.is_num'))
    james = Customer('james', datetime.datetime.now())
    assert james['balance'] == 0
    with pytest.raises(TypeError):
        Customer('james', 'not a date, should fail')
    with pytest.raises(ValueError):
        Customer('james', datetime.datetime.now(), 'not a number so should fail')
    with pytest.raises(NameError):
        Customer = named_dict('Customer', ['name', 'date', 'balance'], defaults = dict(balance = 0), types = dict(date = 'not_a_type'))

def test_named_dict_with_casting():
    Customer = named_dict('Customer', ['name', 'date', 'balance'], 
                          defaults = dict(balance = 0), 
                          types = dict(date = 'datetime.datetime'), 
                          casts = dict(balance = 'float', date = 'pyg.dt'))
    james = Customer('james', 2000, balance = '10.3')
    assert james['balance'] == 10.3
    assert james['date'] == datetime.datetime(2000,1,1)


def test_named_dict_with_other_data_types():
    class Customer(named_dict('Customer', ['name', 'date', 'balance'])):
        def add_to_balance(self, value):
            res = self.copy()
            res.balance += value
            return res

    james = Customer('james', 'date', 10)    
    assert james.add_to_balance(10).balance == 20
    assert pd.Series(james).date == 'date'
    assert dict(james) == {'name': 'james', 'date': 'date', 'balance': 10}
    assert json.dumps(james) == '{"name": "james", "date": "date", "balance": 10}'

    
    class VIP(named_dict('VIP', ['name', 'date'])):
        def some_method(self):
            return 'inheritence between classes works as long as members can share'
    
    vip = VIP(james)
    assert vip.some_method() == 'inheritence between classes works as long as members can share' and vip.name == 'james'
