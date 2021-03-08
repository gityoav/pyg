import pandas as pd
import numpy as np
from pyg.base import pd_to_parquet, pd_read_parquet, is_pd, is_dict, is_series, is_arr, is_date, dt2str, encode, mkdir, tree_to_items
from functools import partial


_parquet = '.parquet'
_npy = '.npy'
_csv = '.csv'
_series = '_is_series'
_root = 'root'
_db = 'db'

__all__ = ['root_path', 'pd_to_csv', 'pd_read_csv', 'parquet_encode', 'parquet_write', 'csv_encode', 'csv_write']

def root_path(doc, root, fmt = None):
    """
    returns a location based on doc
    
    :Example:
    --------------
    >>> root = 'c:/%school/%pupil.name/%pupil.surname/'
    >>> doc = dict(school = 'kings', 
                   pupil = dict(name = 'yoav', surname = 'git'), 
                   grades = dict(maths = 100, physics = 20, chemistry = 80), 
                   report = dict(date = dt(2000,1,1), 
                                 teacher = dict(name = 'adam', surname = 'cohen')
                                 )
                   )
    
    >>> assert root_path(doc, root) == 'c:/kings/yoav/git/'

    The scheme is entirely up to the user and the user needs to ensure what defines the unique primary keys that prevent documents overstepping each other...
    >>> root = 'c:/%school/%pupil.name_%pupil.surname/'
    >>> assert root_path(doc, root) == 'c:/kings/yoav_git/'
    
    >>> root = 'c:/archive/%report.date/%pupil.name.%pupil.surname/'
    >>> assert root_path(doc, root, '%Y') == 'c:/archive/2000/yoav.git/'  # can choose to format dates by providing a fmt.
    """
    items = tree_to_items(dict(doc))
    res = root
    for row in items:
        text = '%' + '.'.join(row[:-1])
        if text in root:
            value = dt2str(row[-1], fmt) if is_date(row[-1]) else str(row[-1])
            res = res.replace(text, '%s'% value)
    if '%' in res:
        raise ValueError('The document did not contain enough keys to determine the path %s'%res)
    return res

def pd_to_csv(value, path):
    """
    A small utility to write both pd.Series and pd.DataFrame to csv files
    """
    assert is_pd(value), 'cannot save non-pd'
    if is_series(value):
        value.index.name = _series
    if value.index.name is None:
        value.index.name = 'index'
    if path[-4:].lower()!=_csv:
        path = path + _csv
    mkdir(path)
    value.to_csv(path)
    return path

def pd_read_csv(path):
    """
    A small utility to read both pd.Series and pd.DataFrame from csv files
    """
    res = pd.read_csv(path)
    if res.columns[0] == _series and res.shape[1] == 2:
        res = pd.Series(res[res.columns[1]], res[_series].values)
        return res
    if res.columns[0] == 'index':
        res = res.set_index('index')
    return res

_pd_read_csv = encode(pd_read_csv)
_pd_read_parquet = encode(pd_read_parquet)
_np_load = encode(np.load)

def parquet_encode(value, path, compression = 'GZIP'):
    """
    encodes a single DataFrame or a document containing dataframes into a an abject that can be decoded

    >>> from pyg import *     
    >>> path = 'c:/temp'
    >>> value = dict(key = 'a', n = np.random.normal(0,1, 10), data = dictable(a = [pd.Series([1,2,3]), pd.Series([4,5,6])], b = [1,2]), other = dict(df = pd.DataFrame(dict(a=[1,2,3], b= [4,5,6]))))
    >>> encoded = parquet_encode(value, path)
    >>> assert encoded['n']['file'] == 'c:/temp/n.npy'
    >>> assert encoded['data'].a[0]['path'] == 'c:/temp/data/a/0.parquet'
    >>> assert encoded['other']['df']['path'] == 'c:/temp/other/df.parquet'

    >>> decoded = decode(encoded)
    >>> assert eq(decoded, value)
    """
    if path.endswith(_parquet):
        path = path[:-len(_parquet)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        return dict(_obj = _pd_read_parquet, path = pd_to_parquet(value, path + _parquet))
    elif is_arr(value):
        np.save(path + _npy, value)
        return dict(_obj = _np_load, file = path + _npy)        
    elif is_dict(value):
        return type(value)(**{k : parquet_encode(v, '%s/%s'%(path,k), compression) for k, v in value.items()})
    elif isinstance(value, (list, tuple)):
        return type(value)([parquet_encode(v, '%s/%i'%(path,i), compression) for i, v in enumerate(value)])
    else:
        return value


def csv_encode(value, path):
    """
    encodes a single DataFrame or a document containing dataframes into a an abject that can be decoded while saving dataframes into csv
    
    >>> path = 'c:/temp'
    >>> value = dict(key = 'a', data = dictable(a = [pd.Series([1,2,3]), pd.Series([4,5,6])], b = [1,2]), other = dict(df = pd.DataFrame(dict(a=[1,2,3], b= [4,5,6]))))
    >>> encoded = csv_encode(value, path)
    >>> assert encoded['data'].a[0]['path'] == 'c:/temp/data/a/0.csv'
    >>> assert encoded['other']['df']['path'] == 'c:/temp/other/df.csv'

    >>> decoded = decode(encoded)
    >>> assert eq(decoded, value)
    """
    if path.endswith(_csv):
        path = path[:-len(_csv)]
    if path.endswith('/'):
        path = path[:-1]
    if is_pd(value):
        return dict(_obj = _pd_read_csv, path = pd_to_csv(value, path))
    elif is_dict(value):
        return type(value)(**{k : csv_encode(v, '%s/%s'%(path,k)) for k, v in value.items()})
    elif isinstance(value, (list, tuple)):
        return type(value)([csv_encode(v, '%s/%i'%(path,i)) for i, v in enumerate(value)])
    else:
        return value
    

            
def parquet_write(doc, root = None):
    """
    MongoDB is great for manipulating/searching dict keys/values. 
    However, the actual dataframes in each doc, we may want to save in a file system. 
    - The DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
    - Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from app
    - MongoDB free version has limitations on size of document
    - file based system may be faster, especially if saved locally not over network
    - for data licensing issues, data must not sit on servers but stored on local computer

    Therefore, the doc encode will cycle through the elements in the doc. Each time it sees a pd.DataFrame/pd.Series, it will 
    - determine where to write it (with the help of the doc)
    - save it to a .parquet file

    """
    if _root in doc:
        root  = doc[_root]
    if root is None and _db in doc and isinstance(doc[_db], partial) and _root in doc[_db].keywords:
        root = doc[_db].keywords[_root]
    if root is None:
        return doc
    path = root_path(doc, root)
    return parquet_encode(doc, path)

def csv_write(doc, root = None):
    """
    MongoDB is great for manipulating/searching dict keys/values. 
    However, the actual dataframes in each doc, we may want to save in a file system. 
    - The DataFrames are stored as bytes in MongoDB anyway, so they are not searchable
    - Storing in files allows other non-python/non-MongoDB users easier access, allowing data to be detached from orignal application
    - MongoDB free version has limitations on size of document
    - file based system may be faster, especially if saved locally not over network
    - for data licensing issues, data must not sit on servers but stored on local computer

    Therefore, the doc encode will cycle through the elements in the doc. Each time it sees a pd.DataFrame/pd.Series, it will 
    - determine where to write it (with the help of the doc)
    - save it to a .csv file

    """
    if _root in doc:
        root  = doc[_root]
    if root is None and _db in doc and isinstance(doc[_db], partial) and _root in doc[_db].keywords:
        root = doc[_db].keywords[_root]
    if root is None:
        return doc
    path = root_path(doc, root)
    return csv_encode(doc, path)


