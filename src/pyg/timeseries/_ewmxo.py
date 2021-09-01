from pyg.base import Dict, is_num
from pyg.timeseries._ewm import ewma_, ewmstd_
from pyg.timeseries._index import sub_, div_
from pyg.timeseries._rolling import v2na 
from pyg.timeseries._expanding import cumsum_

def _frac(days):
    return 1/(1+days) if days>1 else days
        
def ou_factor(fast, slow):
    """
    OU factor for momentum predictions.
    
    Suppose 

    >>> f = 1/(1+fast); F = 1-f; F2 = F^2
    >>> s = 1/(1+slow); S = 1-s; S2 = S^2

    If returns are IID and WLOG ts(0) = 0 we have that (once we flip returns)
    
    >>> ts(-n) = ts(0) + rtn(0) + rtn(-1) + ... rtn(-(n-1))
    >>> fast_ewma(0) = f * ts(0) + f * F ts(-1) + f * F^n ts(-n)
    >>> = f *      (ts0)
    >>> + f * F    (ts(0) + rtn(0))
    >>> + f * F^2  (ts(0) + rtn(0) + rtn(-1))
    >>> + f * F^3  (ts(0) + rtn(0) + rtn(-1) + rtn(-2))
    >>> ...
    
    >>> fast_ewma(0) = ts(0) + F * rtn(0) + F^2 * rtn(-1) + F^3 * rtn(-2) + ...
    >>> slow_ewma(0) = ts(0) + S * rtn(0) + S^2 * rtn(-1) + S^3 * rtn(-2) + ...

    >>> crossover(0) = (F-S) rtn(0) + (F^2-S^2) * rtn(-1)...
    
    The process has zero mean and variance:
        
    >>> E(crossover^2) = \sum_{i>=1} (F^i - S^i)^2 
    >>>                = \sum_{i>=1} (F^2i + S^2i - 2 F^i * S^i)     
    >>>                = F^2 / (1 - F^2) + S^2 / (1-S^2) - 2 F*S / (1-F*S)
    

    Parameters
    ----------
    fast : TYPE
        DESCRIPTION.
    slow : TYPE
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    f = _frac(fast); F = 1-f; F2 = F**2
    s = _frac(slow); S = 1-s; S2 = S**2
    return (F2/(1-F2) + S2/(1-S2) - 2*F*S/(1-F*S)) ** 0.5


def ewmxo_(rtn, fast, slow, vol = None, instate = None):
    """
    This is the normalized crossover function
    
    >>> import numpy as np; import pandas as pd; from pyg import * 
    >>> rtn = pd.Series(np.random.normal(0,1,10000),drange(-9999,0))
    >>> fast = 64; slow = 192; vol = 32; instate = None
    
    """
    state = Dict(fast = {}, slow = {}, vol = {}, cumsum = {}) if instate is None else instate
    ts = cumsum_(rtn, instate = state.get('cumsum'))
    fast_ewma_ = ewma_(ts.data, fast, instate = state.get('fast'))
    slow_ewma_ = ewma_(ts.data, slow, instate = state.get('slow'))
    vol_ = ewmstd_(rtn, vol, instate = state.get('vol')) if is_num(vol) else vol
    signal = sub_(fast_ewma_.data, slow_ewma_.data)
    normalized = div_(signal, v2na(vol_.data) * ou_factor(fast, slow))
    return Dict(data = normalized, state = Dict(fast = fast_ewma_.state, 
                                                slow = slow_ewma_.state, 
                                                vol = vol_.state))

ewmxo_.output = ['data', 'state']


def ewmxo(rtn, fast, slow, vol = None, instate = None):
    """
    This is the normalized crossover function
    
    >>> import numpy as np; import pandas as pd; from pyg import * 
    >>> rtn = pd.Series(np.random.normal(0,1,10000),drange(-9999,0))
    >>> fast = 64; slow = 192; vol = 32; instate = None
    >>> ewmxo(rtn, fast, slow, vol).plot()
    
    """
    return ewmxo_(rtn, fast, slow, vol, instate).data

