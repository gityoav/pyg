from pyg.base import Dict, is_df, is_nums, is_pd, dictable
import numpy as np
import pandas as pd
from pyg.timeseries._index import df_concat
from pyg.timeseries._rolling import init2v, ffill
from pyg.timeseries._ewm import ewma
        


def shape1(value):
    """
    >>> assert shape1(3) == 0
    >>> assert shape1(np.arange(10)) == 0
    >>> assert shape1(np.random.normal(0,1,(10,20))) == 20
    """
    shape = getattr(value, 'shape', ())
    return shape[1] if len(shape)>1 else 0

def _rms_combine(signals, weights, days = 1024, max_leverage = 5):
    """
    
    :Theory:
    -------
    Suppose we have a collctions of RMS1 random variables sig_i with relative weights w_i and we want to aggregate:
        
    >>>  X = \sum w_i sig_i      # Note that w_i can be negative
    
    We would like to normalize X so that X is also RMS1. We use a single factor model:
    
    >>> E(sig_i * sig_j) = rho   # instantenously for all i <> j
    >>> E(X^2) = \sum w_i^2 + rho \sum_{i<>j} w_i w_j 
    
    So 
    
    >>> rho^ = (X^2 - \sum w_i^2) / \sum_{i<>j} w_i w_j  

    is a point estimator for rho at time t and we use recent history as the full history estimator:
    
    >>> rho = ewma(rho^, days)   
    
    :Risk and max leverage:
    -----------------------
    Given rho, we can now estimate X variance based on current sig and weights
    
    >>> var(X) = E(X^2) = \sum w_i^2 + rho \sum_{i<>j} w_i w_j 
    >>> vol(X) = var(X)^0.5    
    
    Now we want to estimate leverage: This is how much absolute risk we have versus risk of X
    
    >>> sum of risks = \sum |w_i|
    >>> risk of sums = vol(X)
    >>> leverage = \sum|w_i| / vol(X)
    
    At times, we want to ensure our vol estimation does not drop to low. So we cap cap maximum leverage. 
    We then use the capped leverage to re-estimate the floored vol. 
        
    >>> vol = \sum|w_i| / leverage 
    >>> mult = 1/vol
    
    Finally, we provide mult, the multiplier that satisfies mult X ~ RMS1:
    
    :Example:
    ---------
    from pyg import * 
    
    >>> dates = drange(-9999)
    >>> signals = ewmxo(np.random.normal(0,1,(10000,10)), 18, 54, vol = 30)
    >>> weights = np.array([np.arange(1,11)]*10000) * 1. / 45.
    >>> pd.DataFrame(signals, dates).plot()
    >>> days = 1024
    >>> max_leverage = 5

    Parameters
    ----------
    signals : TYPE
        DESCRIPTION.
    weights : TYPE
        DESCRIPTION.
    cor_days : TYPE, optional
        DESCRIPTION. The default is 1024.
    max_leverage : TYPE, optional
        DESCRIPTION. The default is 5.

    Returns
    -------
    None.

    """
    mask = np.isnan(signals)
    all_nan = np.min(mask, axis=1)
    signals[mask] = 0.0
    weights[mask] = 0.0
    ws = weights * signals
    ws2 = ws ** 2
    w2 = weights ** 2    
    ws_sum = np.sum(ws, axis=1)
    ws2_sum = np.sum(ws2, axis=1)
    w2_sum = np.sum(w2, axis=1)
    w_abs = np.abs(weights)
    w_abs_sum = np.sum(w_abs, axis=1)
    w_ij = w_abs_sum ** 2 - w2_sum # this has no signal in it
    w_ij[w_ij ==0] = np.nan  
    rho_point = (ws2_sum - w2_sum) / w_ij
    rho_point[all_nan] = np.nan
    rho = ewma(init2v(rho_point, 0.3 * days, 0.0), days)
    rho = np.minimum(rho, 1)
    rho = np.maximum(rho, -1)
    var = np.maximum(w2_sum + rho * w_ij, 0)
    vol = np.sqrt(var)
    leverage = w_abs_sum / vol
    if max_leverage>0:
        leverage = np.minimum(leverage, max_leverage)
        vol = w_abs_sum / leverage
    mult = 1/vol
    signal = ws_sum / vol
    return dict(data = signal, rho = rho, mult = mult, vol = vol, var = var, leverage = leverage)
    



def rms_combine(signals, weights = None, days = 1024, max_leverage = 5, join = 'outer', method = 'ffill'):
    """
    >>> dt1 = drange(2000,2020, '1b')
    >>> sig1 = pd.DataFrame(ewmxo(np.random.normal(0,1,(len(dt1),10)), 18, 54, vol = 30) , dt1)
    >>> sig2 = pd.DataFrame(ewmxo(np.random.normal(0,1,(len(dt1),10)), 18, 54, vol = 30) , dt1)
    >>> signals = [sig1, sig2]    
    >>> rms_combine(signals)
    >>> rms_combine(sig1)

    Parameters
    ----------
    signals : dataframe/list of dataframes
        list of RMS signals
    weights : 
        list of weights
    days : int
        Days for calculating correlation. The default is 1024.
    join : str, optional
        how join is done. The default is 'outer'.
        
        
    :Example: single data frame
    ---------------------------
    >>> from pyg import * 
    >>> dates = drange(2000,2020, '1b')
    >>> signals = pd.DataFrame(ewmxo(np.random.normal(0,1,(len(dates),10)), 18, 54, vol = 30) , dates)
    >>> signals[np.random.normal(0,1,(len(dates),10))>2] = np.nan
    >>> method = 'ffill'; days = 1024
    >>> join = 'outer'
    >>> res = rms_combine(signals)
    >>> res.data.plot()
    
    :Example: single np.ndarray
    ---------
    >>> signals = ewmxo(np.random.normal(0,1,(len(dates),10)), 18, 54, vol = 30)
    >>> res2 = rms_combine(signals)
    
    
    :Example: list of signals
    -------------------------
    >>> rtn = pd.Series(np.random.normal(0,1,len(dates)), dates)
    >>> sig1 = pd.Series(ewmxo(rtn, 5, 15, vol = 30) , dates)
    >>> sig2 = pd.Series(ewmxo(rtn, 10, 30, vol = 30) , dates)
    >>> sig3 = pd.Series(ewmxo(rtn, 20, 60, vol = 30) , dates)
    >>> signals = [sig1, sig2, sig3]    
    >>> weights = None    
    >>> res3 = rms_combine(signals)
    >>> res3.rho.plot()
    
    Returns
    -------
    None.

    """
    if is_df(signals): # we have a single dataframe of multiple signals
        if method == 'ffill':
            signals = ffill(signals)

        index = signals.index
        columns = signals.columns
        s = signals.values.astype(float)

        if weights is None:
            w = 0 * s + 1
        elif isinstance(weights, list):
            if is_nums(weights):
                w = np.array([weights]*len(signals))
            else:
                w = pd.concat(weights, axis = 1).reindex(index, method = 'ffill').values
        elif is_pd(weights):
            w = weights[columns].reindex(index, method = 'ffill').values
        else:
            raise ValueError('dont know how to interpret weights')
        w = w.astype(float)
        res = _rms_combine(s, w, days = days, max_leverage = max_leverage)
        res = {key: pd.Series(value, index) for key, value in res.items()}
        return Dict(res)

    if isinstance(signals, np.ndarray):
        if method == 'ffill':
            signals = ffill(signals)
        s = signals.astype(float)
        
        if weights is None:
            w = 0 * s + 1
        elif isinstance(weights, list):
            if is_nums(weights):
                w = np.array([weights]*len(signals))
            else:
                w = np.array([np.arange(10), np.arange(10)]).T
        elif is_pd(weights):
            w = weights.ffill().values
        else:
            raise ValueError('dont know how to interpret weights')            
        w = w.astype(float)
        return _rms_combine(s, w, days = days, max_leverage = max_leverage)
        

    if isinstance(signals, list):
        ns = set([sig.shape[1] for sig in signals if len(getattr(sig, 'shape', ())) == 2]) - {1,}
        if len(ns) > 1:
            raise ValueError('multiple dataframes with different widths %s'%ns)
        elif len(ns) == 0: # single calculations, this is simple
            sigs = pd.concat(signals, axis=1, join = join)
            return rms_combine(sigs, weights = weights, days = days, max_leverage = max_leverage, join = join, method = method)
        elif len(ns) == 1: # we are combining listed signals, each operates on a multi-column asset
            n = list(ns)[0]
            if weights is None:
                weights = [1.] * len(signals)
            cols = set([tuple(sig.columns) for sig in signals if shape1(sig) == n] + [tuple(wgt.columns) for wgt in weights if shape1(wgt) == n])
            if len(cols)>1:
                raise ValueError('all columns within the list must be the same %s'%cols)
            columns = list(list(cols)[0])
            res = []
            for i in range(n):
                sigs = [sig[i] if shape1(sig) == n else sig for sig in signals]
                wgts = [wgt[i] if shape1(wgt) == n else wgt for wgt in weights]
                res.append(rms_combine(sigs, wgts, days = days, max_leverage = max_leverage, join = join, method = method))            
            res = Dict(dictable(res)).do(lambda value: df_concat(value, columns))
            return res
        
    if isinstance(signals, dict):
        wgts = [weights[key] for key in signals if key in signals] if isinstance(weights, dict) else weights
        sigs = list(signals.values())
        return rms_combine(sigs, wgts, days = days, max_leverage = max_leverage, join = join, method = method)
    
    raise ValueError('dont know how to interpret signals')
    