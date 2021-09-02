import numpy as np
from pyg.timeseries._decorators import compiled


@compiled
def stdev_calculation(t0, t1, t2, default = np.nan):
    if t0 > 1:
        p = t0-1
        return np.sqrt(t2/p - (t1**2)/(p*t0))
    else:
        return default

@compiled
def stdev_calculation_ewm(t0, t1, t2, w2, min_sample, bias = False):
    """
    The nicest calculation of unbiased variance under variable weights is available here:
    https://mathoverflow.net/questions/11803/unbiased-estimate-of-the-variance-of-a-weighted-mean
    """
    if t0<=min_sample:
        return np.nan
    variance = t2/t0 - (t1/t0)**2
    if variance<0:
        return np.nan
    elif bias:
        return np.sqrt(variance)
    else:
        r = 1 - w2/(t0**2)
        return np.sqrt(variance/r)

@compiled
def variance_calculation_ewm(t0, t1, t2, w2, min_sample, bias = False):
    """
    The nicest calculation of unbiased variance under variable weights is available here:
    https://mathoverflow.net/questions/11803/unbiased-estimate-of-the-variance-of-a-weighted-mean
    """
    if t0<=min_sample:
        return np.nan
    variance = t2/t0 - (t1/t0)**2
    if variance<0:
        return np.nan
    elif bias:
        return variance
    else:
        r = 1 - w2/(t0**2)
        return variance/r

@compiled
def cor_calculation_ewm(t0, a1, a2, b1, b2, w2, ab, min_sample, bias = False):
    if t0<=min_sample:
        return np.nan
    Eab = ab/t0
    Ea = a1/t0
    Eb = b1/t0
    STDa = stdev_calculation_ewm(t0, a1, a2, w2, min_sample = min_sample, bias = bias)
    STDb = stdev_calculation_ewm(t0, b1, b2, w2, min_sample = min_sample, bias = bias)
    denom = STDa * STDb
    if denom > 0:
        return (Eab - Ea*Eb)/denom
    else:
        return np.nan

@compiled
def corr_calculation_ewm(ab0, a0, a1, a2, aw2, b0, b1, b2, bw2, ab, min_sample, bias = False):
    if ab0<=min_sample:
        return np.nan
    Eab = ab/ab0
    Ea = a1/a0
    Eb = b1/b0
    STDa = stdev_calculation_ewm(a0, a1, a2, aw2, min_sample = min_sample, bias = bias)
    STDb = stdev_calculation_ewm(b0, b1, b2, bw2, min_sample = min_sample, bias = bias)
    denom = STDa * STDb
    if denom > 0:
        return (Eab - Ea*Eb)/denom
    else:
        return np.nan



@compiled
def LR_calculation_ewm(t0, a1, a2, b1, b2, w2, ab, min_sample, bias = False):
    if t0<=min_sample:
        return np.nan, np.nan
    Eab = ab/t0
    Ea = a1/t0
    Eb = b1/t0
    VARa = variance_calculation_ewm(t0, a1, a2, w2, min_sample = min_sample, bias = bias)
    if VARa > 0:
        covar = Eab - Ea*Eb
        m = covar/VARa
        c = Eb - m * Ea
        return c, m
    else:
        return np.nan, np.nan
    
@compiled
def skew_calculation(t0, t1, t2, t3, bias, min_sample):
    if t0<=max(min_sample,2):
        return np.nan
    t0 = t0 * 1.
    m1 = t1 / t0
    m2 = t2 / t0 - m1 ** 2
    if m2 > 0:
        m3 = (t3/t0 - 3 * m1 * t2 / t0 + 2 * (m1**3))
        biased = m3 / (m2**1.5)
        if bias:
            return biased
        else:
            return biased  * np.sqrt(t0 * (t0-1)) / (t0-2) 
    else:
        return np.nan

