from pyg.base import is_num, is_ts, df_concat
import pandas as pd
import numpy as np
import numba

@numba.njit
def _p(x, y, vol = 0):
    if vol == 0:
        return 1. if x<y else -1. if x>y else 0.0
    else:
        one_sided_tail = 0.5 * np.exp(-abs(y-x)/vol)
        return 1-one_sided_tail if x<y else one_sided_tail

@numba.njit
def _xrank(a, w, b, vol, scale = 0 , reweight = False):
    """
    
    performs a cross-sectional rank
    
    a = np.random.normal(0,1,20)
    a[np.random.normal(0,1,20) > 1] = np.nan
    w = np.full(20, 1.)
    b = np.full(20, 1.)
    scale = 0; vol = -1; reweight = False
    a
    _xrank(a, w, b, vol)
    
    ranks a from -1 to +1 such that:
        
    i)  a[i] < a[j] ==> rank[i] < rank[j]
    ii)  rank[i] in (-1, 1)
    iii) \sum w[i] rank[i] = 0  
    
    Parameters
    ----------
    a : TYPE
        DESCRIPTION.
    w : TYPE
        DESCRIPTION.
    b : TYPE
        DESCRIPTION.
    vol : TYPE
        DESCRIPTION.
    scale : TYPE, optional
        DESCRIPTION. The default is 0.
    reweight : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """
    not_ok = np.isnan(a)
    ok = ~not_ok
    if np.max(not_ok):
        a = a.copy(); w = w.copy(); b = b.copy()
        a[not_ok] = 0.0
        b[not_ok] = 0.0
        w[not_ok] = 0.0
    wb = w * b
    total_wb = np.sum(wb)
    if total_wb == 0:
        return np.full_like(a, np.nan)
    else:
        r = np.zeros_like(a)
        wb = wb / total_wb

    if vol < 0:    
        wba = wb * a
        m1 = np.sum(wba)
        m2 = np.sum(wba * a)
        vol = (m2 - m1**2) ** 0.5

    for i in range(a.shape[0]):
        if ok[i] and w[i]!=0:
            for j in range(i):
                if ok[j] and w[j]!=0:
                    qq = _p(a[i], a[j], vol)
                    pp = 1-qq
                    r[i] += (2*pp-1) * wb[j]
                    r[j] += (2*qq-1) * wb[i]
    
    if scale == 0:
        std = 1
    elif scale == 1: # scale weightes so that total weight = 1
        total_w = np.sum(w)
        w = w / total_w
        std = np.sum((w*r)**2*(1-b**2)) ** 0.5
        r = r/std
    elif scale == 2:
        std = (np.sum(r**2) - np.sum(r)**2) ** 0.5
        r = r/std
    elif scale == 3:
        total_w = np.sum(w)
        w = w / total_w
        std = np.sum(w*(r**2)) ** 0.5
        r = r/std
    
    r[not_ok] = np.nan
    if reweight:
        r = r * w
    return r

@numba.njit
def _xrank_2d(a, w, b, vol, scale, reweight):
    res = np.empty_like(a)
    for i in range(a.shape[0]):
        res[i] = _xrank(a = a[i], w = w[i], b = b[i], vol = vol[i], scale = scale , reweight = reweight)
    return res

def xrank(a, weight = None, beta = None, vol = True, scale = 0 , reweight = False, columns = None):
    """
    
    performs a cross-sectional rank
    
    a = np.random.normal(0,1,20)
    a[np.random.normal(0,1,20) > 1] = np.nan
    w = np.full(20, 1.)
    b = np.full(20, 1.)
    scale = 0; vol = -1; reweight = False
    a
    _xrank(a, w, b, vol)
    
    ranks a from -1 to +1 such that:
        
    i)  a[i] < a[j] ==> rank[i] < rank[j]
    ii)  rank[i] in (-1, 1)
    iii) \sum w[i] rank[i] = 0  
    
    Parameters
    ----------
    a : TYPE
        DESCRIPTION.
    w : TYPE
        DESCRIPTION.
    b : TYPE
        DESCRIPTION.
    vol : TYPE
        DESCRIPTION.
    scale : TYPE, optional
        DESCRIPTION. The default is 0.
    reweight : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.
    
    :Example:
    ---------
    >>> a = pd.DataFrame(np.random.normal(0,1,(1000,20)), drange(-999))
    >>> aa = cumsum(a)
    >>> aa.plot()
    >>> beta = weight = None
    >>> vol = True; scale = 0; columns = None
    >>> res = xrank(aa)
    >>> res.plot()

    """
    a = df_concat(a, columns).ffill()
    index = a.index
    cols = a.columns
    a_ = a.values
    if weight is None:
        w = np.full_like(a_, 1.)
    elif is_num(weight):
        w = np.full_like(a_, weight)
    else:
        w = df_concat(weight, columns).reindex(index, method = 'ffill')
    if beta is None:
        b = np.full_like(a_, 1.)
    elif is_num(beta):
        b = np.full_like(a_, beta)
    else:
        b = df_concat(beta, columns).reindex(index, method = 'ffill')
    if vol is True:
        vol = -1
    if is_ts(vol):
        vol - vol.reindex(index, method = 'ffill')
        if isinstance(vol, pd.DataFrame) and vol.shape[1] == 1:
            vol = vol.iloc[:,0]
    else:
        vol = np.full(a_.shape[0], vol)        
        
    b, w, vol = [df.values if is_ts(df) else df for df in (b,w,vol)]
    res = _xrank_2d(a_, w, b, vol, scale, reweight)
    return pd.DataFrame(res, index, cols)