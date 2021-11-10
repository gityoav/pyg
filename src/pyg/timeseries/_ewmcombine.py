from pyg.base import df_reindex
from pyg.timeseries._rolling import na2v, ffill, v2na
from pyg.timeseries._ewm import ewma, ewmcorr, ewmstd
import numpy as np

def _ewmcombine(a, w, n = 1024, vol_days = None, full = False):
    """
    We assume all the joining together etc. is done
    
    Parameters
    ----------
    a : np.array
        2-dimensional array of signals
    w : np.ndarray
        2-dimensional array of weights

    Returns
    -------
    

    :Math:
    ------
    
    We have a collection of signals that we assume are RMS-1, i.e. N(0,1)-ish.
    Our relative weights w on each of the signals changes over time, perhaps because 
    - we allocate differently or 
    - because some signals only live at certain times of the year.


    We would like to create a variable X:
    X = \sum_i w_i * a_i

    But we also want to scale it so that X is RMS-1
    Since X is not necessarily RMS-1, we would want to divide it by its own vol estimate, which we can do with ewmrms(X, n)
    
    However, this does not take into account the fact that weights can change over time so the vol estimate yesterday may be irrelevant as w have changed.


    Single factor model
    -------------------        
    So let us assume a single factor correlation rho(t)

    E(X) = 0
    E(X^2) = X2 = \sum w_i^2 + \sum_{i<>j} w_i w_j rho(t)
        
    W1 = \sum w_i
    W2 = \sum w_i^2
    WIJ =  \sum_{i<>j} w_i w_j = \sum(i,j) w_i w_j - W2 = W1**2 - W2
    
    We reverse the equation and express rho as a function of E(X^2) 
    rho(t) = (X2 - W2)/WIJ
    
    This is a point estimate for rho. Once we have rho, we can estimate the unknown variance of X at each point of time.

    Full factor model
    -----------------
    Here we use ewmcorr to estimate a running correlation between all the signals. If we don't have a value (e.g. this is a recent timeseries so no data as yet)
    then we default to rho(t) from the single factor model above.
    
    Once we have the full correlation matrix estimation, we can estimate variance as...
    E(X^2) = w^T * C * w

    """
    a_ = na2v(ffill(a))
    w_ = na2v(ffill(w))
    if vol_days:
        vols = v2na(ffill(ewmstd(a, vol_days)))
        a_ = a_ / vols
    else:
        vols = 1
    x = np.sum(a_ * w_, axis = 1)
    x2 = x**2
    w1 = np.sum(w_, axis = 1)
    w2 = np.sum(w_ ** 2, axis = 1)
    wij = w1 ** 2 - w2
    rho = (x2 - w2)/wij
    erho = ewma(rho, n) # this gives us our estimate for rho at time t
    if full:
        c = ewmcorr(a, n)
        c_ = np.zeros(c.shape)
        variance = np.empty(c.shape[0])
        cip = np.zeros(c.shape[1:])
        for i in range(c.shape[0]):
            ci = c[i]
            wi = w_[i]
            ci[np.isnan(ci)] = cip[np.isnan(ci)]
            ci[np.isnan(ci)] = erho[i]
            variance[i] = np.matmul(wi, np.matmul(ci,wi))
            c_[i] = ci
    else:
        variance = w2 + wij * erho # and hence an estimate for variance
    variance[variance<=0] = np.nan
    vol = ffill(np.sqrt(variance))
    mult = 1/vol
    data = x/vol
    return dict(data = data, vol = vol, rho = erho, mult = mult, cor = c_ if full else None, vols = vols if vol_days else 1)

def ewmcombine(a, w, n = 1024, full = False, join = 'ij', method = None):
    df_reindex(a, )
    