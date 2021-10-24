from pyg import drange, Calendar, calendar, clock, dt, date_range, DAY, timer, eq, as_time, TMIN, TMAX, dt_bump
import datetime
import pandas as pd; import numpy as np
import pytest
from pyg.base._drange import _quarter_clock

t0 = dt(0)
def test_date_range():
    assert date_range(-10) == [dt(0) - 10 * DAY, dt(0)]
    assert date_range(10) == [dt(0), dt(0) + 10 * DAY]
    assert date_range(2000, 10) == [dt(2000,1,1), dt(2000) + 10 * DAY]
    assert date_range('20000101', 10) == [dt(2000,1,1), dt(2000) + 10 * DAY]
    assert date_range(-3, '1w') == [dt(0) - 3*DAY, dt(0) + 7 * DAY]


def test_calendar_date_range():
    cal = calendar()
    cal = Calendar(cal)
    assert cal.date_range() == [TMIN, dt(0)]
    assert cal.date_range(dt(2000)) ==  [datetime.datetime(2000, 1, 1, 0, 0), dt(0)]
    assert cal.date_range(dt(2300)) ==  [dt(0), datetime.datetime(2300, 1, 1, 0, 0)]
    assert cal.date_range(None, 1000) == [dt(0), dt(dt(0), 1000)]
    assert cal.date_range('-1y', 2000) == [dt(1999), dt(2000)]
    assert cal.date_range(None, 2300) == [dt(0), dt(2300)]
 
def test_calendar_weekend():
    isr = calendar('israel', weekend = (4,5))
    sunday = dt(2021,2,22)
    friday = dt(2021,2,20)
    assert isr.is_bday(sunday)
    assert not isr.is_bday(friday)


def test_drange_intraday():
    assert drange(dt(2000), dt_bump(2000, '35h'), '7h') == [datetime.datetime(2000, 1, 1, 0, 0),
                                                             datetime.datetime(2000, 1, 1, 7, 0),
                                                             datetime.datetime(2000, 1, 1, 14, 0),
                                                             datetime.datetime(2000, 1, 1, 21, 0),
                                                             datetime.datetime(2000, 1, 2, 4, 0),
                                                             datetime.datetime(2000, 1, 2, 11, 0)]

    assert drange(dt(2000), dt_bump(2000, '35n'), '7n') == [datetime.datetime(2000, 1, 1, 0, 0),
                                                             datetime.datetime(2000, 1, 1, 0, 7),
                                                             datetime.datetime(2000, 1, 1, 0, 14),
                                                             datetime.datetime(2000, 1, 1, 0, 21),
                                                             datetime.datetime(2000, 1, 1, 0, 28),
                                                             datetime.datetime(2000, 1, 1, 0, 35)]


def test_drange_simple():
    assert drange(2000, 9) == [datetime.datetime(2000, 1, 1, 0, 0),
                                 datetime.datetime(2000, 1, 2, 0, 0),
                                 datetime.datetime(2000, 1, 3, 0, 0),
                                 datetime.datetime(2000, 1, 4, 0, 0),
                                 datetime.datetime(2000, 1, 5, 0, 0),
                                 datetime.datetime(2000, 1, 6, 0, 0),
                                 datetime.datetime(2000, 1, 7, 0, 0),
                                 datetime.datetime(2000, 1, 8, 0, 0),
                                 datetime.datetime(2000, 1, 9, 0, 0),
                                 datetime.datetime(2000, 1, 10, 0, 0)]

    assert drange(2000, 9, '1b') == [datetime.datetime(2000, 1, 3, 0, 0),
                                     datetime.datetime(2000, 1, 4, 0, 0),
                                     datetime.datetime(2000, 1, 5, 0, 0),
                                     datetime.datetime(2000, 1, 6, 0, 0),
                                     datetime.datetime(2000, 1, 7, 0, 0),
                                     datetime.datetime(2000, 1, 10, 0, 0)]

    assert drange(2000, '5w', '1w') == [datetime.datetime(2000, 1, 1, 0, 0),
                                         datetime.datetime(2000, 1, 8, 0, 0),
                                         datetime.datetime(2000, 1, 15, 0, 0),
                                         datetime.datetime(2000, 1, 22, 0, 0),
                                         datetime.datetime(2000, 1, 29, 0, 0),
                                         datetime.datetime(2000, 2, 5, 0, 0)]

    with pytest.raises(ValueError):
        drange(2000,1999, 1)
    with pytest.raises(ValueError):
        drange(2000,1999, '1b')
    for bump in ['3b', '7b', '8b', '11b']:
        assert drange(2000,dt(2000,2,1), bump)[0] == dt(2000,1,3)
    for bump in ['-4b', '-6b', '-8b', '-11b']:
        assert drange(dt(2000,2,1), 2000, bump)[-1] != dt(2000,1,3) 

def test_drange_bump_timedelta():
    bump = datetime.timedelta(hours = 12)
    t0 = dt(2020); t1 = dt(2020,1,2)
    assert drange(t0, t1, bump) == [datetime.datetime(2020, 1, 1, 0, 0),
                             datetime.datetime(2020, 1, 1, 12, 0),
                             datetime.datetime(2020, 1, 2, 0, 0)]
    assert drange(t1, t0, -bump) == [datetime.datetime(2020, 1, 1, 0, 0),
                             datetime.datetime(2020, 1, 1, 12, 0),
                             datetime.datetime(2020, 1, 2, 0, 0)][::-1]

    with pytest.raises(ValueError):
        drange(t0, t1, -bump) 
    with pytest.raises(ValueError):
        drange(t1, t0, bump) 
    assert drange(t0, t0, bump) == [t0]


    
    
def test_calendar_drange():
    cal = calendar('US', holidays = [dt(2000,1,4), dt(2000,1,10)])
    assert cal.drange(2000, '9b', '1b') == [datetime.datetime(2000, 1, 3, 0, 0),
                                             datetime.datetime(2000, 1, 5, 0, 0),
                                             datetime.datetime(2000, 1, 6, 0, 0),
                                             datetime.datetime(2000, 1, 7, 0, 0),
                                             datetime.datetime(2000, 1, 11, 0, 0),
                                             datetime.datetime(2000, 1, 12, 0, 0),
                                             datetime.datetime(2000, 1, 13, 0, 0),
                                             datetime.datetime(2000, 1, 14, 0, 0),
                                             datetime.datetime(2000, 1, 17, 0, 0),
                                             datetime.datetime(2000, 1, 18, 0, 0)]

    assert cal.bdays(dt(2000), dt(2000,1,2)) == 0
    assert cal.bdays(dt(2000), dt(2000,1,6)) == cal.bdays(dt(2000,1,3), dt(2000,1,6)) == 2

    assert cal.dt_bump(dt(2000), '0b') == dt(2000,1,3)
    assert cal.dt_bump(dt(2000), 0) == dt(2000,1,1)
    assert cal.dt_bump(dt(2000), '2b') == dt(2000,1,6)
    
    assert len(cal.drange(2000,2001,'10b')) == 27 and len(drange(2000,2001,'10b')) == 27
        
    

def test_calendar_implementation_is_faster_than_rrule():
    cal = calendar('US', holidays = [dt(2000,1,4), dt(2000,1,10)])
    cal._populate()
    assert timer(lambda : cal.drange(2000,2010,'1b'), 10, True)() < timer(lambda : drange(2000,2010,'1b'), 10, True)()
    assert timer(lambda : cal.drange(2000,2010,'10b'), 10, True)() < timer(lambda : drange(2000,2010,'10b'), 10, True)()


def test_clock():
    ts = pd.Series(np.arange(11), drange(2000,10)) * 1.
    cal = calendar('US', holidays = [dt(2000,1,4), dt(2000,1,10)])
    assert eq(clock(ts) , np.full(len(ts), np.nan))
    assert eq(clock(ts, 'i') , np.arange(1,12))
    assert eq(clock(ts.values, 'i') , np.arange(1,12))
    assert eq(clock(ts.values, 'i', 5) , np.arange(6,17))
    assert eq(clock(ts.values, None, 5) , np.full(len(ts), np.nan))
    assert eq(clock(ts.values) , np.full(len(ts), np.nan))
    assert eq(clock(ts, 'b') , np.array([26090, 26090, 26090, 26091, 26092, 26093, 26094, 26095, 26095, 26095, 26096]))
    assert eq(clock(ts, cal) , np.array([26090, 26090, 26090, 26091, 26091, 26092, 26093, 26094, 26094, 26094, 26094]))
    assert eq(clock(ts, 'y') , np.array([2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000]))
    assert eq(clock(ts, 'm') , np.array([2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000])*12 + 1)
    assert eq(clock(ts, 'q') , np.array([2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000])*4)
    with pytest.raises(ValueError):
        clock(ts.values, 'm')
    with pytest.raises(ValueError):
        clock(ts.values, 'b')
    with pytest.raises(ValueError):
        clock(ts.values, 'y')
    ts[dt(2000,1,6)] = np.nan
    assert eq(clock(ts), np.full(len(ts), np.nan))

    for m, q in zip(range(1,13), [0,0,0,1,1,1,2,2,2,3,3,3]): 
        assert _quarter_clock(dt(2000,m,1)) == 8000 + q 


def test_calendar_trade_date():
    uk = calendar('UK')
    assert uk.trade_date(dt(2021,2,9,5), 'f', day_start = 8, day_end = 17) == dt(2021, 2, 9)  # Tuesday morning rolls into Tuesday
    assert uk.trade_date(dt(2021,2,9,5), 'p', day_start = 8, day_end = 17) == dt(2021, 2, 8)  # Tuesday morning back into Monday
    assert uk.trade_date(dt(2021,2,7,5), 'f', day_start = 8, day_end = 17) == dt(2021, 2, 8)  # Sunday rolls into Monday
    assert uk.trade_date(dt(2021,2,7,5), 'p', day_start = 8, day_end = 17) == dt(2021, 2, 5)  # Sunday rolls back to Friday
    
    assert uk.trade_date(date = dt(2021,2,9,23), adj = 'f', day_start = 8, day_end = 17) == dt(2021, 2, 10)  # Tuesday eve rolls into Wed
    assert uk.trade_date(date = dt(2021,2,9,23), adj = 'p', day_start = 8, day_end = 17) == dt(2021, 2, 9)  # Tuesday eve back into Tuesday
    assert uk.trade_date(date = dt(2021,2,7,23), adj = 'f', day_start = 8, day_end = 17) == dt(2021, 2, 8)  # Sunday rolls into Monday
    assert uk.trade_date(date = dt(2021,2,7,23), adj = 'p', day_start = 8, day_end = 17) == dt(2021, 2, 5)  # Sunday rolls back to Friday
    
    assert uk.trade_date(date = dt(2021,2,9,12), adj = 'f', day_start = 8, day_end = 17) == dt(2021, 2, 9)  # Tuesday is Tuesday
    assert uk.trade_date(date = dt(2021,2,9,12), adj = 'p', day_start = 8, day_end = 17) == dt(2021, 2, 9)  # Tuesday is Tuesday
    
    au = calendar('AU')
    assert au.trade_date(dt(2021,2,9,5), 'f', day_start = 2230, day_end = 1300) == dt(2021, 2, 9)  # Tuesday morning in session
    assert au.trade_date(dt(2021,2,9,5), 'p', day_start = 2230, day_end = 1300) == dt(2021, 2, 9)  # Tuesday morning in session
    assert au.trade_date(dt(2021,2,7,5), 'f', day_start = 2230, day_end = 1300) == dt(2021, 2, 8)  # Sunday rolls into Monday
    assert au.trade_date(dt(2021,2,7,5), 'p', day_start = 2230, day_end = 1300) == dt(2021, 2, 5)  # Sunday rolls back to Friday
    
    assert au.trade_date(date = dt(2021,2,9,23), adj = 'f', day_start = 2230, day_end = 1300) == dt(2021, 2, 10)  # Tuesday eve rolls into Wed
    assert au.trade_date(date = dt(2021,2,9,23), adj = 'p', day_start = 2230, day_end = 1300) == dt(2021, 2, 10)  # Already in Wed
    assert au.trade_date(date = dt(2021,2,7,23), adj = 'f', day_start = 2230, day_end = 1300) == dt(2021, 2, 8)  # Sunday rolls into Monday
    assert au.trade_date(date = dt(2021,2,7,23), adj = 'p', day_start = 2230, day_end = 1300) == dt(2021, 2, 8)  # Already on Monday
    assert au.trade_date(date = dt(2021,2,5,23), adj = 'f', day_start = 2230, day_end = 1300) == dt(2021, 2, 8)  # Friday afternoon rolls into Monday
    
    assert au.trade_date(date = dt(2021,2,9,14), adj = 'f', day_start = 2230, day_end = 1300) == dt(2021, 2, 10)  # Tuesday is over, roll to Wed
    assert au.trade_date(date = dt(2021,2,9,14), adj = 'p', day_start = 2230, day_end = 1300) == dt(2021, 2, 9)  # roll back to Tues

def test_as_time():
    t0 = as_time(dt()) 
    t = as_time(None)
    t1 = as_time(dt())
    assert t0<=t and t<=t1
    t0 = as_time(dt()) 
    t = as_time()
    t1 = as_time(dt())
    assert t0<=t and t<=t1
    assert as_time('10:30:40') == datetime.time(10, 30, 40)
    assert as_time('103040') == datetime.time(10, 30, 40)
    assert as_time('10:30') == datetime.time(10, 30)
    assert as_time('1030') == datetime.time(10, 30)
    assert as_time('05') == datetime.time(5)
    assert as_time(103040) == datetime.time(10, 30, 40)
    assert as_time(13040) == datetime.time(1, 30, 40)
    assert as_time(130) == datetime.time(1, 30)
    assert as_time(datetime.time(1, 30)) == datetime.time(1, 30)
    assert as_time(datetime.datetime(2000, 1, 1, 1, 30)) == datetime.time(1, 30)
    with pytest.raises(ValueError):
        as_time('what am I?')
    with pytest.raises(ValueError):
        as_time(dict(a=1))

def test_drange():
    assert drange(t0,t0) == [t0]
    with pytest.raises(ValueError):
        drange(1950, 2000, -1)
    with pytest.raises(ValueError):
        drange(2000, 1900, 1)
        drange(2000, 1999, -1)
    
def test_calendar__repr__():
    cal = calendar('US')
    assert cal.__repr__() == 'calendar(US) from 1900-01-01 00:00:00 to 2300-01-01 00:00:00'
    cal = calendar('UK', t0 = 2000, t1 = 2025)
    assert cal.__repr__() == 'calendar(UK) from 2000-01-01 00:00:00 to 2025-01-01 00:00:00'
    
    
def test_calendar_is_trading():
    cal = calendar('test')
    assert cal.is_trading(dt(2021,3,1))
    assert cal.is_trading(dt(2021,3,1,10,20,40))
    assert not cal.is_trading(dt(2021,2,28))

    assert not cal.is_trading(dt(2021,3,1), day_start = 7, day_end = 18)
    assert cal.is_trading(dt(2021,3,1,8), day_start = 7, day_end = 18)
    assert cal.is_trading(dt(2021,3,1,18), day_start = 7, day_end = 18)
    assert not cal.is_trading(dt(2021,3,1,19), day_start = 7, day_end = 18)

    assert cal.is_trading(dt(2021,3,1), day_start = 20, day_end = 7)
    assert not cal.is_trading(dt(2021,3,1,8), day_start = 20, day_end = 7) ## Monday post close
    assert cal.is_trading(dt(2021,2,28,23), day_start = 20, day_end = 7) ## Sunday night


def test_calendar_modified_adj():
    cal = calendar('test_calendar_modified_adj')
    assert cal.adjust(dt(2021,2,21)) == dt(2021,2,22) ## following
    assert cal.adjust(dt(2021,2,28)) == dt(2021,2,26) ## except at month end, go to previous
    assert cal.drange(2000,20,1) == drange(2000,20,1)
    assert cal.drange(2000,20,'1b') == drange(2000,20,'1b')

def test_calendar_filter():
    cal = calendar()
    dts = pd.Series(drange(-1000, dt(1999)), drange(-1000, dt(1999)))
    bts = pd.Series(cal.drange(-1000, dt(1999), '1b'), cal.drange(-1000, dt(1999), '1b'))
    assert eq(cal.filter(dts), bts)
    assert len(cal.filter(bts)) == len(bts)



