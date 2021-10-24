import pandas as pd
import numpy as np
from _collections_abc import Iterable, dict_keys, dict_values
import datetime
from functools import partial

from pyg.base._logger import logger

__all__ = ['is_series', 'is_df', 'is_pd', 'is_arr', 'is_list', 'is_len', 'is_tuple', 'is_int', 'is_float', 'is_num', 'is_bool', 'is_str', 'is_nan', 'is_none', 'is_dict', 'is_iterable', 'is_date', 'is_ts']
__all__ += ['is_pds', 'is_arrs', 'is_ints', 'is_floats', 'is_nums', 'is_bools', 'is_strs', 'is_nans', 'is_nones', 'is_dicts', 'is_iterables', 'is_dates', 'is_tss', 'is_lists', 'is_tuples']
__all__ += ['nan2none', 'NoneType']
__all__ = sorted(__all__)
NoneType = type(None)

def is_series(value):
    """ is value a pd.Series"""
    return isinstance(value, pd.Series)

def is_df(value):
    """ is value a pd.DataFrame"""
    return isinstance(value, pd.DataFrame)

    
def is_pd(value):
    """ is value a pd.DataFrame/pd.Series"""
    return isinstance(value, (pd.Series, pd.DataFrame))

def is_arr(value):
    """ is value a numpy array of non-zero-size"""
    return isinstance(value, np.ndarray) and len(value.shape) > 0

is_array = is_arr

def is_int(value):
    """ is value an int, or any variant of np.intN type"""
    return isinstance(value, (int, np.int64, np.int32, np.int16, np.int8))

def is_float(value):
    """ is value an float, or any variant of np.float """
    return isinstance(value, (float, np.float16, np.float32, np.float64))

def is_num(value):
    """ is _int(value) or is_float(value)"""
    return is_int(value) or is_float(value)

def is_bool(value):
    """ is value a Bool, or a np.bool_ type"""
    return isinstance(value, (bool, np.bool_))

def is_str(value):
    """ is value a str, or a np.str_ type"""
    return isinstance(value, (str, np.str_))

def is_nan(value):
    """ is value a nan or an inf. Unlike np.isnan, works for non numeric"""
    return is_float(value) and (np.isnan(value) or np.isinf(value))

def is_none(value):
    """ is value None"""
    return value is None

def is_dict(value):
    """ is value a dict"""
    return isinstance(value, dict)

def is_zero_len(value):
    """ is value of zero length (or has no len at all)"""
    return getattr(value ,'__len__', lambda : 0)() == 0

def is_len(value):
    """ is value of zero length (or has no len at all)"""
    return getattr(value ,'__len__', lambda : 0)() > 0

def is_iterable(value):
    """is value Iterable excluding a string"""
    return isinstance(value, Iterable) and not is_str(value)


def nan2none(value):
    """convert np.nan/np.inf to None"""
    return None if is_nan(value) else value

def is_list(value):
    """is value a list"""
    return isinstance(value, list)

def is_tuple(value):
    """is value a tuple"""
    return isinstance(value, tuple)

def is_date(value):
    """is value a date type: either datetime.date, datetime.datetime or np.datetime64"""
    return isinstance(value, (datetime.date, datetime.datetime, np.datetime64))


def is_primitive(value):
    return value is None or is_bool(value) or is_num(value) or is_date(value) or is_str(value)


def is_ts(value):
    """is value a pandas datafrome whch is indexed by datetimes"""
    if is_pd(value):
        if len(value):
            t0, t1 = value.index[0], value.index[-1]
            if is_date(t0) and is_date(t1):
                if t0>t1:
                    logger.warning('WARN: unsorted timeseries %s...%s'%(t0, t1))
                return True
            else:
                return False
        else:
            return True
    else:
        return False
                    
def is_one_or_many(value, check):
    return check(value) or (is_iterable(value) and is_len(value) and check(value[0]) and min([check(v) for v in value]))


is_pds = partial(is_one_or_many, check = is_pd) 

is_arrs = partial(is_one_or_many, check = is_arr) 

is_ints = partial(is_one_or_many, check = is_int) 

is_floats = partial(is_one_or_many, check = is_float) 

is_nums = partial(is_one_or_many, check = is_num) 

def is_strs(value):
    return is_str(value) or (isinstance(value, (list, tuple, dict_keys, dict_values)) and is_len(value) and min([is_str(v) for v in value]))

is_nans = partial(is_one_or_many, check = is_nan) 

is_dicts = partial(is_one_or_many, check = is_dict) 

def is_lists(value):
    check = is_list
    return is_iterable(value) and is_len(value) and check(value[0]) and min([check(v) for v in value])

is_bools = partial(is_one_or_many, check = is_bool) 

is_nones = partial(is_one_or_many, check = is_none) 

is_dates = partial(is_one_or_many, check = is_date)

is_iterables = partial(is_one_or_many, check = is_iterable) 

is_tss = partial(is_one_or_many, check = is_ts) 

is_tuples = partial(is_one_or_many, check = is_tuple) 
