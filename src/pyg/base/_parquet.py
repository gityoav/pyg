from pyg.base._file import mkdir, path_name
from pyg.base._types import is_series, is_df, is_int, is_date, is_bool, is_str, is_float
from pyg.base._dates import dt2str, dt
from pyg.base._logger import logger
import pandas as pd
import jsonpickle as jp

_series = '_is_series'

__all__ = ['pd_to_parquet', 'pd_read_parquet']


def pd_to_parquet(value, path, compression = 'GZIP'):
    """
    a small utility to save df to parquet, extending both pd.Series and non-string columns    

    :Example:
    -------
    >>> from pyg import *
    >>> import pandas as pd
    >>> import pytest

    >>> df = pd.DataFrame([[1,2],[3,4]], drange(-1), columns = [0, dt(0)])
    >>> s = pd.Series([1,2,3], drange(-2))

    >>> with pytest.raises(ValueError): ## must have string column names
            df.to_parquet('c:/temp/test.parquet')

    >>> with pytest.raises(AttributeError): ## pd.Series has no to_parquet
            s.to_parquet('c:/temp/test.parquet')
    
    >>> df_path = pd_to_parquet(df, 'c:/temp/df.parquet')
    >>> series_path = pd_to_parquet(s, 'c:/temp/series.parquet')

    >>> df2 = pd_read_parquet(df_path)
    >>> s2 = pd_read_parquet(series_path)

    >>> assert eq(df, df2)
    >>> assert eq(s, s2)

    """
    if is_series(value):
        mkdir(path)
        df = pd.DataFrame(value)
        df.columns = [_series]
        try:
            df.to_parquet(path, compression = compression)
        except ValueError:
            df = pd.DataFrame({jp.dumps(k) : [v] for k,v in dict(value).items()})
            df[_series] = True
            df.to_parquet(path, compression = compression)
        return path
    elif is_df(value):
        mkdir(path)
        df = value.copy()
        df.columns = [jp.dumps(col) for col in df.columns]
        df.to_parquet(path, compression  = compression)
        return path
    else:
        return value        

def pd_read_parquet(path):
    """
    a small utility to read df/series from parquet, extending both pd.Series and non-string columns 

    :Example:
    -------
    >>> from pyg import *
    >>> import pandas as pd
    >>> import pytest

    >>> df = pd.DataFrame([[1,2],[3,4]], drange(-1), columns = [0, dt(0)])
    >>> s = pd.Series([1,2,3], drange(-2))

    >>> with pytest.raises(ValueError): ## must have string column names
            df.to_parquet('c:/temp/test.parquet')

    >>> with pytest.raises(AttributeError): ## pd.Series has no to_parquet
            s.to_parquet('c:/temp/test.parquet')
    
    >>> df_path = pd_to_parquet(df, 'c:/temp/df.parquet')
    >>> series_path = pd_to_parquet(s, 'c:/temp/series.parquet')

    >>> df2 = pd_read_parquet(df_path)
    >>> s2 = pd_read_parquet(series_path)

    >>> assert eq(df, df2)
    >>> assert eq(s, s2)

    """
    path = path_name(path)
    try:
        df = pd.read_parquet(path)
    except Exception:
        logger.warning('WARN: unable to read pd.read_parquet("%s")'%path)
        return None
    if df.columns[-1] == _series:
        if len(df.columns) == 1:
            res = df[_series]
            res.name = None
            return res
        else:
            return pd.Series({jp.loads(k) : df[k].values[0] for k in df.columns[:-1]})
    else:
        df.columns = [jp.loads(col) for col in df.columns]
        return df

