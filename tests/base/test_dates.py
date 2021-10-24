from pyg import dt, dt_bump, drange, dt,dt_bump, today, ymd, TMIN, TMAX, DAY, futcodes, dt2str, eq
from pyg.base._dates import none2dt, is_period, ym, month, uk2dt, us2dt
import datetime
import pytest
import dateutil as du
d = datetime.datetime
t = dt.now()
t0 = d(t.year, t.month, t.day)
import numpy as np
import pandas as pd

def test_month():
    assert month(1) == 1
    assert month(1.) == 1
    assert month('h') == 3
    assert month('m') == 6
    assert month('jun') == 6
    with pytest.raises(ValueError):
        month('fail')
    with pytest.raises(ValueError):
        month(dict(a = 1))


def test_none2dt():
    now = dt()
    assert none2dt() >= now
    assert none2dt(lambda : dt(0)) == dt(0)
    assert none2dt(0) == 0
    
    
def test_dt_date():
    t = datetime.date(2010,1,1)
    assert dt(t) == datetime.datetime(2010,1,1)
    
    
def test_dt_bump():
    for bmp in ['1d', '1m', '-3w', '4b', '1h', '2n', '6s']:
        assert dt(bmp) == dt_bump(0, bmp)
        
def test_dt2str_empty():
    assert dt2str(dt(2010,2,3), '') == '20100203'
    
def test_dt_utc():
    t = dt()
    timestamp = t.timestamp()
    assert dt(timestamp) == datetime.datetime.utcfromtimestamp(timestamp)
    if datetime.datetime.utcfromtimestamp(timestamp) == t:
        assert dt(timestamp) == t

def test_ym():
    assert ym(2000, -1) == (1999,11)
    assert ym(2000, 0) == (1999,12)
    assert ym(2000, 1) == (2000,1)
    assert ym(2000, 'h') == (2000,3)
    assert ym(2000, 'March') == (2000,3)
    assert ym(2000, 12) == (2000,12)
    assert ym(2000, 13) == (2001,1)


def test_ymd():
    assert ymd(2000, -1,1) == d(1999,11,1)
    assert ymd(2000, 0,1) == d(1999,12,1)
    assert ymd(2000, 1,1) == d(2000,1,1)
    assert ymd(2000, 'h',1) == d(2000,3,1)
    assert ymd(2000, 'March',1) == d(2000,3,1)
    assert ymd(2000, 12,1) == d(2000,12,1)
    assert ymd(2000, 13,1) == d(2001,1,1)
    
    assert ymd(2000, 0,0) == dt(1999,11,30)


def test_dt_dialect():
    with pytest.raises(ValueError):
        dt('02-15-2020')
    with pytest.raises(ValueError):
        dt('15-02-2020', dialect = 'us')

    assert dt('02-15-2020', dialect= 'us')  == dt(2020, 2,15)
    assert dt('15-02-2020', dialect= 'uk')  == dt(2020, 2,15)
    assert dt('null', dialect= 'us') is None    
    assert dt('null', dialect= 'uk') is None    
    

def test_dt_bump_bad_tenor():

    t = dt(2000)
    with pytest.raises(ValueError):
        dt_bump(t, 'whatever')

    class fake_bump():
        def __add__(self, other):
            return 'whatever'
        __radd__ = __add__
    assert dt_bump(t, fake_bump()) == 'whatever'


def test_dt_none_or_nan():
    assert dt(None, none = None) is None
    assert dt(None, none = 1)  == 1
    assert dt(None, none = lambda : dt(0)) == dt(0)
    assert dt(np.nan, none = None) is None
    assert dt(np.nan, none = 1)  == 1
    assert dt(np.nan, none = lambda : dt(0)) == dt(0)
    
    with pytest.raises(ValueError):
        dt(dict())



def test_dt():
    d = datetime.datetime
    assert dt('01-02-2002') == datetime.datetime(2002, 2, 1)
    assert dt('01-02-2002', dialect = 'US') == datetime.datetime(2002, 1, 2)
    assert dt('01 March 2002') == datetime.datetime(2002, 3, 1)
    assert dt('01 March 2002', dialect = 'US') == datetime.datetime(2002, 3, 1)
    assert dt('01 March 2002 10:20:30') == datetime.datetime(2002, 3, 1, 10, 20, 30)
    assert dt(20020301) == datetime.datetime(2002, 3, 1)
    assert dt(37316) == datetime.datetime(2002, 3, 1) # excel dates
    assert dt(2000) == datetime.datetime(2000,1,1)
    assert dt(2000,3) == datetime.datetime(2000,3,1)
    assert dt(2000,3, 1) == datetime.datetime(2000,3,1)
    assert dt(2000,3, 1, 10,20,30) == datetime.datetime(2000,3,1,10,20,30)
    assert dt(2000,'march', 1) == datetime.datetime(2000,3,1)
    assert dt(2000,'h', 1) == datetime.datetime(2000,3,1) # future codes
    assert dt(2000,'h', 0) == datetime.datetime(2000,2,29) # future codes
    assert dt(d(2000,3,1).toordinal()) == datetime.datetime(2000,3,1)
    assert dt() == d.now()
    assert dt.now() == d.now()
    assert dt(0) == t0
    assert dt(100) == t0 + datetime.timedelta(100)
    assert dt(-100) == t0 + datetime.timedelta(-100)
    assert dt(t0,'1w') == t0 + datetime.timedelta(7)

def test_np2dt():
    d = datetime.datetime(2000,1,1,20,30,40,55)
    t = np.datetime64(d)    
    assert dt(t) == d
    t = np.datetime64(d).astype('datetime64[D]')
    assert dt(t) == ymd(d)
    t = np.datetime64(d).astype('datetime64[m]')
    assert dt(t) == datetime.datetime(2000,1,1,20,30)
    t = np.datetime64(d).astype('datetime64[ms]')
    assert dt(t) == datetime.datetime(2000,1,1,20,30,40)
    t = np.datetime64(d).astype('datetime64[ns]')
    assert dt(t) == d

def test_dt2str():
    t = dt(2000,3,1)
    assert dt2str(t) == '20000301'
    assert dt2str(t, 'iso') == '2000-03-01T00:00:00'
    assert dt2str(t, '-') == '2000-03-01'
    assert dt2str(t, 'y-b') == '00-Mar'
    assert dt2str(t, 'd-B-Y') == '01-March-2000'
    t = 20000301
    assert dt2str(t) == '20000301'
    assert dt2str(t, 'iso') == '2000-03-01T00:00:00'
    assert dt2str(t, '-') == '2000-03-01'
    assert dt2str(t, 'y-b') == '00-Mar'
    assert dt2str(t, 'd-B-Y') == '01-March-2000'
    t = datetime.datetime(2000,1,1,20,30,40,55)
    assert dt2str(t) == '2000-01-01T20:30:40.000055'


def test_dt_bump_ts():
    t  = pd.Series([1,2,3], drange(dt(2000,1,1),2))
    assert eq(dt_bump(t, 1), pd.Series([1,2,3], drange(dt(2000,1,2),2)))
    

def test_dt_bump():
    t = dt(2000,3,1)
    assert dt_bump(t, 1) == d(2000,3,2)
    assert dt_bump(t,datetime.timedelta(1)) == d(2000,3,2)
    delta = du.relativedelta.relativedelta(hours = 20) 
    assert dt_bump(t, delta) == t + delta == dt(2000,3,1,20)
    assert dt_bump(t, -1) == d(2000,2,29)
    assert dt_bump(t, '1d') == d(2000,3,2)
    assert dt_bump(t, '-1d') == d(2000,2,29)
    assert dt_bump(t, '1m') == d(2000,4,1)
    assert dt_bump(t, '-1m') == d(2000,2,1)
    assert dt(t, 1) == d(2000,3,2)
    assert dt(t,datetime.timedelta(1)) == d(2000,3,2)
    assert dt(t, delta) == t + delta == dt(2000,3,1,20)
    assert dt(t, -1) == d(2000,2,29)
    assert dt(t, '1d') == d(2000,3,2)
    assert dt(t, '-1d') == d(2000,2,29)
    assert dt(t, '1m') == d(2000,4,1)
    assert dt(t, '-1m') == d(2000,2,1)

    t = dt(2000,3,30)
    assert dt_bump(t, '-1m') == dt(2000,2,30)
    assert dt_bump(t, '-1w') == dt(2000,3,23)
    assert dt_bump(t, '1q') == dt(2000,6,30)
    assert dt_bump(t, '2y') == dt(2002,3,30)

    assert dt_bump(t, '1b') == dt(2000,3,31)
    assert dt_bump(t, '2b') == dt(2000,4,3)

    assert dt(t, '-1m') == dt(2000,2,30)
    assert dt(t, '-1w') == dt(2000,3,23)
    assert dt(t, '1q') == dt(2000,6,30)
    assert dt(t, '2y') == dt(2002,3,30)

    assert dt(t, '1b') == dt(2000,3,31)
    assert dt(t, '2b') == dt(2000,4,3)


def test_drange():
    
    assert drange(2000,2001,'1m') == [datetime.datetime(2000, 1, 1, 0, 0),
                                     datetime.datetime(2000, 2, 1, 0, 0),
                                     datetime.datetime(2000, 3, 1, 0, 0),
                                     datetime.datetime(2000, 4, 1, 0, 0),
                                     datetime.datetime(2000, 5, 1, 0, 0),
                                     datetime.datetime(2000, 6, 1, 0, 0),
                                     datetime.datetime(2000, 7, 1, 0, 0),
                                     datetime.datetime(2000, 8, 1, 0, 0),
                                     datetime.datetime(2000, 9, 1, 0, 0),
                                     datetime.datetime(2000, 10, 1, 0, 0),
                                     datetime.datetime(2000, 11, 1, 0, 0),
                                     datetime.datetime(2000, 12, 1, 0, 0),
                                     datetime.datetime(2001, 1, 1, 0, 0)]

    assert drange([dt(2000),'-5b'], 2000 ,'1b') == [datetime.datetime(1999, 12, 27, 0, 0),
                                                     datetime.datetime(1999, 12, 28, 0, 0),
                                                     datetime.datetime(1999, 12, 29, 0, 0),
                                                     datetime.datetime(1999, 12, 30, 0, 0),
                                                     datetime.datetime(1999, 12, 31, 0, 0)]

    assert drange([dt(2000),'-5b'], '5b','1b') == [datetime.datetime(1999, 12, 27, 0, 0),
                                                     datetime.datetime(1999, 12, 28, 0, 0),
                                                     datetime.datetime(1999, 12, 29, 0, 0),
                                                     datetime.datetime(1999, 12, 30, 0, 0),
                                                     datetime.datetime(1999, 12, 31, 0, 0), datetime.datetime(2000, 1, 3, 0, 0)]

    day = DAY
    assert drange(-5) == [t0 - 5*day, t0 - 4*day, t0 - 3*day, t0-2*day, t0-day, t0]
    assert drange(5) == [t0 + 5*day, t0 + 4*day, t0 + 3*day, t0 + 2*day,  t0 + day, t0][::-1]
    

