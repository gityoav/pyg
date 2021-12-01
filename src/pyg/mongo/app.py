from flask import Flask, request
from pyg.base import dt, encode, is_date, dt2str
from pyg.mongo._db_cell import get_data
import json
import numpy as np
import pandas as pd


app = Flask(__name__)

def _cast(value):
    if isinstance(value, str):
        if value.startswith('int('):
            return int(value[4:-1]) 
        elif value.startswith('dt('):
            return dt(value[3:-1]) 
        elif value.startswith('float('):
            return float(value[6:-1]) 
    return value

def _encode(value):
    if isinstance(value, (list,tuple)):
        return type(value)([_encode(v) for v in value])
    elif isinstance(value, np.ndarray) and len(value.shape) == 1:
        return dict(_obj = encode(np.array), object = _encode(list(value)))
    elif isinstance(value, pd.Series):
        return dict(_obj = encode(pd.Series), data = _encode(value.values), index = _encode(value.index.values))
    elif is_date(value):
        return dt2str(value)
    elif isinstance(value, pd.DataFrame):
        # value = pd.DataFrame([[1,2], [3,4]], drange(1), ['a', 'b'])
        values = {key: _encode(value[key].values) for key in value.columns}
        index = _encode(value.index.values)
        return dict(_obj = encode(pd.DataFrame), data = values, index = index)
    elif isinstance(value, dict):
        return type(value)({_encode(key): _encode(v) for key, v in value.items()})        
    else:
        return encode(value)

    
@app.route('/<db>/<table>', methods = ['GET'])
def request_data(db, table):
    """
    A generic end-point to the mongo database. 
    The interface allows any cell data to be accessed.
    The Endpoint supppots http://webpage/db/table?key1=value1&key2=value2
    same as a Rest access via the GET method.
    
    You need Flask installed to run this file. 
    - conda install flask
    - cd to pyg/src/pyg/mongo directry
    - flask run
    
    
    :Params:
    --------
    db: str
        name of mongo db
    
    table: str
        name of mongo collection
    
    params: dict
        keys that define the cell uniquely. 
        can be expressed as: dict(key1 = value1, key2 = value2)
        
        HOWEVER: int/float/dates are converted to string at request. 
        
        >>> params = dict(date = dt(2020,1,1), age = 35) # bad, will fail to filter

        replace with:
            
        >>> dict(json = json.dumps(params)) can be safely used to send int/dates keys
        
        or if you prefer:
            
        >>> params = dict(date = 'dt(20200101)', age = 'int(35)') # strings that will be evaluated by server

    
    :Example:
    ---------
    We first upload data to database
    
    >>> from pyg import * 
    >>> from pyg import decode, mongo_table, db_cell, dt
    >>> import requests
    >>> import json

    >>> db = partial(mongo_table, db = 'test', table = 'test', pk = 'key')
    >>> c = db_cell(data = 10, db = db, key = 'ten').save()
    >>> c = db_cell(data = 'five', db = db, key = 5).save()
    >>> s = db_cell(data = pd.Series([1,2,3], [4,5,6]), db = db, key = 'series').save()

    Now we read it from flask
    make sure flask run app.py is running
    
    >>> import requests
    >>> params = dict(key = 'ten')
    >>> args = dict(json = json.dumps(params))
    >>> r = requests.get('http://127.0.0.1:5000/test/test', params = args) 
    >>> assert r.json() == 10

    >>> r = requests.get('http://127.0.0.1:5000/test/test', params = dict(key = 'int(5)')) 
    >>> assert r.json() == 'five'

    >>> r = requests.get('http://127.0.0.1:5000/test/test', params = dict(key = 'series')) 
    >>> assert eq(decode(r.json()), s.data)
    
    :Example: more complicated objects
    ---------
    The objects travels in json land so it needs some TLC on the receiving end...        
 
    >>> params = dict(mkt = 'YS', m = 'Z', y = 2020)
    >>> args = dict(json = json.dumps(params))
    >>> r = requests.get('http://127.0.0.1:5000/eoddata/contracts', params = args) 
    
    >>> res = decode(r.json())
    >>> res.index = [dt(v) for v in res.index]
    >>> res.iloc[:3]

    >>>             Close   High    Low   Open  OpenInt Symbol  Volume
    >>> 2020-03-23  715.5  715.5  715.5  715.5        0  YSZ20       0
    >>> 2020-03-24  750.0  750.0  715.5  715.5        0  YSZ20       0
    >>> 2020-03-25  772.8  772.8  750.0  750.0        0  YSZ20       0
    
    
    >>> import requests
    >>> params = dict(mkt = 'X', item = 'close')
    >>> r = requests.get('http://127.0.0.1:5000/eoddata/markets', params = params) 
    >>> res = decode(r.json())
    >>> res.index = [dt(v) for v in res.index]
    >>> res.plot()
    """
    if request.method == 'GET':
        args = dict(request.args)
        if len(args) == 1 and 'json' in args:
            args = json.loads(args['json'])
        args = {key: _cast(value) for key, value in args.items()}
        res = get_data(db = db, table = table, **args)
        return _encode(res)
    else:
        raise ValueError('please specify a GET request, we dont support PUT at the moment')
        
