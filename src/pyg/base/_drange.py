from pyg.base._dates import dt, DAY, TMIN, TMAX, dt_bump, is_period, is_bump, ymd
from pyg.base._types import is_int, is_str, is_ts, is_arr, is_pd, is_nan
from pyg.base._as_list import as_list 
from pyg.base._dict import Dict
import datetime
from dateutil.rrule import rrule, MONTHLY, WEEKLY, DAILY, YEARLY, MO, TU, WE, TH, FR, SA, SU
from dateutil import tz
from pytz import country_timezones
from functools import lru_cache
import functools
import dateutil as du


import numpy as np
import re

weekdays = {0: MO, 1: TU, 2: WE, 3: TH, 4: FR, 5: SA, 6: SU}

HOUR = datetime.timedelta(hours = 1)
minute = datetime.timedelta(minutes = 1)
second = datetime.timedelta(seconds = 1)
microsecond = datetime.timedelta(microseconds = 1)
isoformat = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}T')

    
__all__ = ['date_range', 'drange', 'calendar', 'Calendar', 'clock', 'as_time']

@lru_cache
def tzones():
    x = {iso :country_timezones[iso] for iso in country_timezones}
    tzs = {iso: code[0] for iso, code in x.items() if len(code) == 1}
    for iso, code in x.items():
        tzs.update({c.split('/')[-1].replace('_', ' ').lower(): c for c in code})
    tzs  = {key : tz.gettz(value) for key, value in tzs.items()}
    t = datetime.datetime.now()
    tzs.update({v.tzname(t): v for v in tzs.values() if v is not None})
    tzs['WET'] = tzs['western european'] = tzs['MA'] # Malta
    tzs['EET'] = tzs['eastern european'] = tzs['GR']
    tzs['CET'] = tzs['cental european'] = tzs['AT'] # austria
    tzs['EST'] = tzs['eastern standard'] = tzs['new york']
    tzs['CST'] = tzs['central standard'] = tzs['chicago']
    tzs['greenwich mean time'] = tzs['GMT']
    return tzs

def as_time(t = None):
    """
    parses t into a datetime.time object
    
    :Example:
    ---------
    >>> assert as_time('10:30:40') == datetime.time(10, 30, 40)
    >>> assert as_time('103040') == datetime.time(10, 30, 40)
    >>> assert as_time('10:30') == datetime.time(10, 30)
    >>> assert as_time('1030') == datetime.time(10, 30)
    >>> assert as_time('05') == datetime.time(5)
    >>> assert as_time(103040) == datetime.time(10, 30, 40)
    >>> assert as_time(13040) == datetime.time(1, 30, 40)
    >>> assert as_time(130) == datetime.time(1, 30)
    >>> assert as_time(datetime.time(1, 30)) == datetime.time(1, 30)
    >>> assert as_time(datetime.datetime(2000, 1, 1, 1, 30)) == datetime.time(1, 30)
    
    :Parameters:
    ----------
    t : str/int/datetime.time/datetime.datetime
        time of day

    :Returns:
    -------
    datetime.time

    """
    if t is None:
        t = dt()
    if is_str(t):
        hms = t.split(':')
        if len(hms) == 3:
            h,m,s = hms
            return datetime.time(int(h), int(m), int(s))
        elif len(hms) == 2:
            h,m = hms
            return datetime.time(int(h), int(m))
        else:
            t = int(t)
    if is_int(t):
        if t<100:
            return datetime.time(t)
        elif t<10000:
            h = t // 100
            m = t % 100
            return datetime.time(h, m)
        elif t<1000000:
            h = t // 10000
            t = t - h * 10000
            m = t // 100
            s = t % 100
            return datetime.time(h,m,s)
    elif isinstance(t, datetime.time):
        return t
    elif isinstance(t, datetime.datetime):
        return datetime.time(t.hour, t.minute, t.second)
    else:
        raise ValueError('cannot parse time %s'%t)

def drange(t0 = None, t1 = None, bump = None):
    """
    A quick and happy wrapper for dateutil.rrule

    :Examples:
    --------------
    >>> drange(2000, 10, 1) # 10 days starting from dt(2000,1,1)
    >>> drange(2000, '10b', '1b') # weekdays between dt(2000,1,1) and dt(2000,1,17)
    >>> drange('-10b', 0, '1b') # business days since 10 bdays ago
    >>> drange('-10b', '10b', '1w') # starting 10b days ago, to 10b from now, counting in weekly jumps
    
    :Parameters:
    ----------------
    t0 : date, optional
        start date. The default is None.
    t1 : date, optional
        end date. The default is None.
    bump : timedelta, int, string, optional
        bump period. The default is None.

    :Returns:
    -------
    list of dates
    
    :Example:
    ---------
    >>> t0 = 2000; t1 = 1999
    >>> bump = '-1b'
    
    :Example:
    ---------
    >>> t0 = dt(2020); t1 = dt(2021); bump = datetime.timedelta(hours = 4)

    """
    t0, t1 = date_range(t0, t1)  
    if t0 == t1:
        return [t0]        
    if bump is None:
        bump = 1 if t0<t1 else -1
    if is_int(bump):
        if (t1-t0).days * bump <= 0:
            raise ValueError('cannot go from %s to %s in steps of %s'%(t0,t1,bump))
        freq = DAILY
        res = list(rrule(freq, interval = 1, dtstart = min(t0,t1), until = max(t0,t1))) # unfortunately does not actually work for negative bumps
        res = res[::-1] if bump<0 else res
        res = res[::abs(bump)] if abs(bump)>1 else res
        return res
    elif isinstance(bump, datetime.timedelta):
        t = t0
        res = []
        if t1>t0: 
            if t0 + bump <= t0:
                raise ValueError('cannot move forward from %s to %s using %s'%(t0, t1, bump))
            while t<=t1:
                res.append(t)
                t = t + bump
            return res
        elif t1<t0: 
            if t0 + bump >= t0:
                raise ValueError('cannot move back from %s to %s using %s'%(t0, t1, bump))
            while t>=t1:
                res.append(t)
                t = t + bump
            return res
        else:
            return [t0]
    elif is_period(bump):
        bump = bump.lower()
        period = bump[-1]
        interval = int(bump[:-1]) * dict(q = 3).get(period,1)
        if (t1-t0).days * interval <= 0:
            raise ValueError('cannot go from %s to %s in steps of %s'%(t0,t1,bump))
        freq = dict(b = DAILY, d = DAILY, w = WEEKLY, m = MONTHLY, q = MONTHLY, y = YEARLY)[period]
        if period == 'b':
            res = [t for t in rrule(freq, interval = 1, dtstart = min(t0,t1), until = max(t0,t1)) if t.weekday()<5]
            res = res[::-1] if interval<0 else res    
            res = res[::abs(interval)] if abs(interval)>1 else res
            return res            
        else:
            return list(rrule(freq, interval = interval, dtstart = t0, until = t1))


class _calendar():
    def dt_bump(self, t, bump):
        return dt_bump(t, bump)
    
    def date_range(self, t0 = None, t1 = None):
        """
        creates a date range boundaries from start/end points
    
        :Parameters:
        ----------------
        t0 : date or a date bump, optional
            start date. The default is None.
        t1 : date or a date bump, optional
            end date. The default is None.
    
        :Returns:
        -------
        sorted list of two dates
        
        
        :Examples:
        --------------
        >>> t = dt.today()
        >>> assert date_range() == [dt(1900), t]
        >>> assert date_range(-100, 100) == [dt_bump(t, -100), dt_bump(t,100)]
        >>> assert date_range(-100, '10b') == [dt_bump(t, -100), dt_bump(t,'10b')]
        >>> assert date_range(2000, '10b') == [dt(2000,1,1), dt(2000,1,17)]
    
        """
        t = dt(0)
        if t1 is None:
            if t0 is None:
                return [TMIN, t]
            elif is_bump(t0):
                return sorted([t, self.dt_bump(t, t0)])
            else:
                return sorted([t, dt(t0)])
        elif is_bump(t1):
            if t0 is None:
                return [t, self.dt_bump(t, t1)]
            elif is_bump(t0):
                return [self.dt_bump(t, t0), dt_bump(t, t1)]
            else:
                t0 = dt(t0)
                return [t0, self.dt_bump(t0, t1)]
        else:
            t1 = dt(t1)
            if t0 is None:
                return [t, t1]
            elif is_bump(t0):
                return [self.dt_bump(t1, t0), t1]
            else:
                return [dt(t0), t1]


_c  = _calendar()

def date_range(t0 = None, t1 = None):
    return _c.date_range(t0, t1)
        

"""
calendars is implemented as a factory of calendars indexed by key

We therefore choose to make any modifications to the calendar object in-place
"""
calendars = dict()

class Calendar(Dict, _calendar):
    """
    Calendar is 
        - a dict 
        - containing holiday dates 
        - implementing business day arithmetic

    Calendar is restricted to operate between cal.t0 and cal.t1 which default to TMIN = 1900 and TMAX = 2300

    Calendar does this by having two key members:
        - dt2int: a mapping from all business dates to their integer 'clock'
        - int2dt: a mapping from integer value to the date
    
    Since Calendar is an 'expensive' memory wise, we assign a key to the calendar and the Calendar is stored in the singleton calendars under this key
    
    :Example:
    -------
    >>> from pyg import *
    >>> holidays = dictable([[1,'2012-01-02','New Year Day',],
                            [2,'2012-01-16','Martin Luther King Jr. Day',],
                            [3,'2012-02-20','Presidents Day (Washingtons Birthday)',],
                            [4,'2012-05-28','Memorial Day',],
                            [5,'2012-07-04','Independence Day',],
                            [6,'2012-09-03','Labor Day',],
                            [7,'2012-10-08','Columbus Day',],
                            [8,'2012-11-12','Veterans Day',],
                            [9,'2012-11-22','Thanksgiving Day',],
                            [10,'2012-12-25','Christmas Day',],
                            [11,'2013-01-01','New Year Day',],
                            [12,'2013-01-21','Martin Luther King Jr. Day',],
                            [13,'2013-02-18','Presidents Day (Washingtons Birthday)',],
                            [14,'2013-05-27','Memorial Day',],
                            [15,'2013-07-04','Independence Day',],
                            [16,'2013-09-02','Labor Day',],
                            [17,'2013-10-14','Columbus Day',],
                            [18,'2013-11-11','Veterans Day',],
                            [19,'2013-11-28','Thanksgiving Day',],
                            [20,'2013-12-25','Christmas Day',],
                            [21,'2014-01-01','New Year Day',],
                            [22,'2014-01-20','Martin Luther King Jr. Day',],
                            [23,'2014-02-17','Presidents Day (Washingtons Birthday)',],
                            [24,'2014-05-26','Memorial Day',],
                            [25,'2014-07-04','Independence Day',],
                            [26,'2014-09-01','Labor Day',],
                            [27,'2014-10-13','Columbus Day',],
                            [28,'2014-11-11','Veterans Day',],
                            [29,'2014-11-27','Thanksgiving Day',],], ['i', 'date', 'name']).do(dt, 'date')

    
    >>> cal = calendar('US', holidays.date, t0 = 2012, t1 = 2015)
    >>> assert not cal.is_bday(dt(2013,9,2))        # Labor day

    >>> cached_calendar = calendar('US')
    >>> assert not cached_calendar.is_bday(dt(2013,9,2))   # Labor day

    >>> assert cal.adjust(dt(2013,9,2)) == dt(2013,9,3)
    >>> assert cal.drange(dt(2013,9,0), dt(2013,9,7), '1b') == [dt(2013,8,30), dt(2013,9,3), dt(2013,9,4), dt(2013,9,5), dt(2013,9,6),] ## skipped labour day and weekend prior
    
    >>> assert cal.bdays(dt(2013,9,0), dt(2013,9,7)) == 5
    """
    def __init__(self, key = None, holidays = None, weekend = None, t0 = None, t1 = None, adj = 'm'):
        """
        :Parameters:
        ----------------
        key : str, None, optional
            The key used to cache the calendar in singleton. The default is None.
        holidays : list of dates
            list of holidays
        weekend : list of int, optional
            What days are weekend 0=MON... 6 = SUN. The default is [5,6].
        t0 : datetime, optional
            calendar's first date. The default is TMIN = 1900.
        t1 : datetime, optional
            calendar's last date. The default is TMIN = 2300.
        adj : char, optional
            how non-bdays are adjusted. The default is 'm'.
            Three conventions are used:
                'f'ollowing : next date
                'p'revious: previous date
                'm'odified following: following date unless following date is in a different month, when we use previous.

        :Returns:
        -------
        Calendar object.

        """
        if isinstance(key, dict):
            super(Calendar, self).__init__(**key)
        if isinstance(key, Calendar):
            super(Calendar, self).__init__(key)
        else:
            if t1 is None:
                t1 = TMAX
            if t0 is None:
                t0 = TMIN
            t0, t1 = date_range(t0, t1)
            ## weekend are weekend days
            if weekend is None: 
                weekend = [5,6]
            else:
                weekend = as_list(weekend)
            holidays = as_list(holidays)
            holidays = dict(zip(holidays, holidays)) # we prefer to store holidays as a dict, as check of date in holidays is faster for hash
            super(Calendar, self).__init__(weekend = weekend, holidays = holidays,
                                           key = key, t0 = t0, t1 = t1, adj = adj)


    def _populate(self):
        """
        This is run once as it is quite expensive
        """
        if self.get('dt2int') is None or self.get('int2dt') is None:
            byweekday = tuple(v for k,v in weekdays.items() if k not in self.weekend)
            bdays = [date for date in rrule(DAILY, interval = 1, dtstart = self.t0, until = self.t1, byweekday = byweekday) if date not in self.holidays]
            self['dt2int'] = dict(zip(bdays, range(len(bdays))))        
            self['int2dt'] = dict(zip(range(len(bdays)), bdays))
        return self
        
    
    def __repr__(self):
        return 'calendar(%(key)s) from %(t0)s to %(t1)s'%self
    
    def is_holiday(self, date):
        return date.weekday() in self.weekend or ymd(date) in self.holidays
    
    def is_bday(self, date):
        return date.weekday() not in self.weekend and ymd(date) not in self.holidays

    def is_trading(self, date = None, day_start = 0, day_end = 235959):
        """
        calculates if we are within a trading session 

        :Parameters:
        ----------
        date : datetime, optional
            the time & date we want to check. The default is None (i.e. now)

        :Returns:
        -------
        bool:
            are we within a trading session
        """
        tod = as_time(date)
        day_start = as_time(day_start); day_end = as_time(day_end)
        day = ymd(date)
        if day_end >= day_start:
            return tod <= day_end and tod >= day_start and self.is_bday(day)
        else:
            if tod>=day_start and self.is_bday(day + DAY): # we are within tomorrow's session
                return True
            elif tod<=day_end and self.is_bday(day): # we are within today's session
                return True
            else:
                return False

    def trade_date(self, date = None, adj = None, day_start = 0, day_end = 235959):
        """
        This is very similar for adjust, but it also takes into account the time of the day.
        if day_start = 0 and day_end = 23:59:59 then this is exactly adjust.

        :Parameters:
        ----------
        date : datetime, optional
            date (with time). The default is None.
        adj : f/p, optional
            If date isn't within trading day, which direction to adjust to? The default is None.
            
        
        :Example:
        ---------
        >>> from pyg import *; import datetime

        >>> uk = calendar('UK', day_start = 8, day_end = 17)
        >>> assert uk.trade_date(dt(2021,2,9,5), 'f') == dt(2021, 2, 9)  # Tuesday morning rolls into Tuesday
        >>> assert uk.trade_date(dt(2021,2,9,5), 'p') == dt(2021, 2, 8)  # Tuesday morning back into Monday
        >>> assert uk.trade_date(dt(2021,2,7,5), 'f') == dt(2021, 2, 8)  # Sunday rolls into Monday
        >>> assert uk.trade_date(dt(2021,2,7,5), 'p') == dt(2021, 2, 5)  # Sunday rolls back to Friday

        >>> assert uk.trade_date(date = dt(2021,2,9,23), adj = 'f') == dt(2021, 2, 10)  # Tuesday eve rolls into Wed
        >>> assert uk.trade_date(date = dt(2021,2,9,23), adj = 'p') == dt(2021, 2, 9)  # Tuesday eve back into Tuesday
        >>> assert uk.trade_date(date = dt(2021,2,7,23), adj = 'f') == dt(2021, 2, 8)  # Sunday rolls into Monday
        >>> assert uk.trade_date(date = dt(2021,2,7,23), adj = 'p') == dt(2021, 2, 5)  # Sunday rolls back to Friday

        >>> assert uk.trade_date(date = dt(2021,2,9,12), adj = 'f') == dt(2021, 2, 9)  # Tuesday is Tuesday
        >>> assert uk.trade_date(date = dt(2021,2,9,12), adj = 'p') == dt(2021, 2, 9)  # Tuesday is Tuesday

        >>> au = calendar('AU', day_start = 2230, day_end = 1300)
        >>> assert au.trade_date(dt(2021,2,9,5), 'f') == dt(2021, 2, 9)  # Tuesday morning in session
        >>> assert au.trade_date(dt(2021,2,9,5), 'p') == dt(2021, 2, 9)  # Tuesday morning in session
        >>> assert au.trade_date(dt(2021,2,7,5), 'f') == dt(2021, 2, 8)  # Sunday rolls into Monday
        >>> assert au.trade_date(dt(2021,2,7,5), 'p') == dt(2021, 2, 5)  # Sunday rolls back to Friday

        >>> assert au.trade_date(date = dt(2021,2,9,23), adj = 'f') == dt(2021, 2, 10)  # Tuesday eve rolls into Wed
        >>> assert au.trade_date(date = dt(2021,2,9,23), adj = 'p') == dt(2021, 2, 10)  # Already in Wed
        >>> assert au.trade_date(date = dt(2021,2,7,23), adj = 'f') == dt(2021, 2, 8)  # Sunday rolls into Monday
        >>> assert au.trade_date(date = dt(2021,2,7,23), adj = 'p') == dt(2021, 2, 8)  # Already on Monday
        >>> assert au.trade_date(date = dt(2021,2,5,23), adj = 'f') == dt(2021, 2, 8)  # Friday afternoon rolls into Monday

        >>> assert au.trade_date(date = dt(2021,2,9,14), adj = 'f') == dt(2021, 2, 10)  # Tuesday is over, roll to Wed
        >>> assert au.trade_date(date = dt(2021,2,9,14), adj = 'p') == dt(2021, 2, 9)  # roll back to Tues
            
        """
        # first we split it into day and time of day
        tod = as_time(date)
        day = ymd(date)
        adj = (adj or self.adj)[0].lower()
        day_start = as_time(day_start); day_end = as_time(day_end)        
        if day_end >= day_start:
            res = self.adjust(day, adj)
            if tod > day_end: # post today trading session
                if adj == 'f':
                    if res > day:
                        return res
                    else:
                        return self.add(res, 1)
                elif adj == 'p':
                    return res
            elif tod < day_start: # pre today session
                if adj == 'f':
                    return res
                elif adj == 'p':
                    if res < day:
                        return res
                    else:
                        return self.add(res, -1)
            else: # in session. 
                return res
        else:
            if tod > day_start: # we are in tomorrow's trading session
                tomorrow = day + datetime.timedelta(1)
                res = self.adjust(tomorrow, adj)
                return res
            elif tod < day_end: # we are in today's trading session
                res = self.adjust(day , adj)
                return res
            else: # we are in post-session
                res = self.adjust(day, adj)
                if adj == 'f':
                    if res > day:
                        return res
                    else:
                        return self.add(res, 1)
                elif adj == 'p':
                    return res

    
    def adjust(self, date, adj = None):
        """

        adjust a non-business day to prev/following bussiness date

        :Parameters:
        ----------------
        date : datetime.
        adj : None or p/f/m
            adjustment convention: 'prev/following/modified following'

        :Returns:
        -------
        dateime
            nearby business day

        """
        adj = adj or self.adj
        t = ymd(date)
        t = datetime.datetime(t.year, t.month, t.day)
        if adj.startswith('f'):
            while self.is_holiday(t) and t <= self.t1:
                t = t + DAY
            while t > self.t1 and t.weekay() in self.weekend:
                t = t + DAY
            return t
                
        elif adj.startswith('p'):
            while self.is_holiday(t) and t >= self.t0:
                t = t - DAY
            while t < self.t0 and t.weekay() in self.weekend:
                t = t - DAY
            return t
        elif adj.startswith('m'):
            t = self.adjust(date, 'f')
            if t.month!=date.month:
                return self.adjust(date, 'p')
            else:
                return t

    def dt_bump(self, t, bump, adj = None):
        """
        adds a bump to a date

        :Parameters:
        ----------------
        t : datetime
            date to bump.
        bump : int, str
            bump e.g. '-1y' or '1b' or 3
        adj : adjustement type
            The default is None.

        :Returns:
        -------
        datetime
            bumped date.

        """
        if is_str(bump) and bump.lower().endswith('b'):
            return self.add(t, int(bump[:-1]))
        else:
            return dt_bump(t, bump)
        
    def clock(self, date):
        self._populate()
        date = ymd(date)
        return self.dt2int.get(date, self.dt2int[self.adjust(date)])

    def add(self, date, days, adj = None):
        """
        adjustes the start date to a business day. Then add business days on top.
        add will initiate self._populate unless we are bumping date by exactly one day

        Parameters
        ----------
        date : datetime
            start date.
        days : int
            days to bump.
        adj : str, optional
            adjusting method for date if it isn't a business day to start with.

        Returns
        -------
        datetime
            bumped date.
        """
        adj = adj or self.adj
        t = self.adjust(date, adj)
        if abs(days)>1:
            self._populate()
            return self.int2dt[self.dt2int[t] + days]
        else:
            increment = days * DAY
            res = t + increment
            while self.is_holiday(res):
                res = res + increment
        return res
    
    def bdays(self, t0, t1, adj = None):
        self._populate()
        adj = adj or self.adj
        return self.dt2int[self.adjust(t1, adj)] - self.dt2int[self.adjust(t0, adj)]
    
    def drange(self, t0 = None, t1 = None, bump = None):
        t0, t1 = self.date_range(t0, t1)
        if is_str(bump) and bump[-1] == 'b':
            self._populate()
            b = int(bump[:-1])
            i0 = self.dt2int[self.adjust(t0)]
            i1 = self.dt2int[self.adjust(t1)]
            return [self.int2dt[i] for i in range(i0, i1+b, b)]
        else:
            return drange(t0, t1, bump)

        
def calendar(key = None, holidays = None, weekend = None, t0 = None, t1 = None):
    """
    A function to returns either an existing calendar or construct a new one.
    - calendar('US') will return a US calendar if that is already cached
    - calendar('US', us_holiday_dates) will construct a calendar with holiday dates and then cache it
    """
    if key not in calendars or holidays is not None or weekend is not None or t0 is not None or t1 is not None:
        calendars[key] = Calendar(key, holidays = holidays, weekend = weekend, t0 = t0, t1 = t1)    
    return calendars[key]

@np.vectorize
def _fraction_clock(t):
    t = dt(t)
    return t.toordinal() + t.hour / 24 + t.minute / 1440 + (t.second + t.microsecond/1e6) / 86400

@np.vectorize
def _day_clock(t):
    t = ymd(t)
    return t.toordinal() 

@np.vectorize
def _week_clock(t):
    t = ymd(t)
    return t.toordinal() // 7

@np.vectorize
def _month_clock(t):
    t = ymd(t)
    return t.year*12 + t.month

@np.vectorize
def _year_clock(t):
    t = ymd(t)
    return t.year

@np.vectorize
def _quarter_clock(t):
    """
    >>> for m, q in zip(range(1,13), [0,0,0,1,1,1,2,2,2,3,3,3]):
    >>>     assert _quarter_clock(dt(2000,m,1)) == 8000 + q
    """
    t = dt(t)
    return t.year * 4 + (t.month-1) // 3

_clocks = dict(f = _fraction_clock, 
               d = _day_clock,
               w = _week_clock,
               m = _month_clock,
               y = _year_clock,
               q = _quarter_clock
               )


def clock(ts, time = None, t = None):
    """
    returns a vector marking the passage of time.

    :Parameters:
    ----------------
    ts : timeseries
    time : None, a string or a Calendar, or already a timeseries of times
        None: Will increment by 1 every non-nan observation
        'i' : increment by 1 every date in index (nan or not)
        'b' : weekdays distance
        'd' : day-distance (ignore intraday stamp)
        'f' : fraction-of-day-distance (do not ignore intraday stamp)
        'm' : month-distance
        'q' : quarter-distance
        'y' : year-distance
        calendar: uses the business-days distance between any two dates
        
    t: starting time in the past. 
    
    :Returns:
    -------
    an array 
        an increasing array of time such that distance between points match the above.
        
        
    :Example:
    -------
    >>> from pyg import *
    >>> assert eq(clock(pd.Series(np.arange(10), drange(2000, 9))), np.arange(1,11))
    >>> assert eq(clock(pd.Series(np.arange(10), drange(2000, 9)), t = 5), np.arange(6,16))
    >>> assert eq(clock(pd.Series(np.arange(10), drange(2000, 9)), 'i'), np.arange(1,11))
    >>> assert eq(clock(pd.Series(np.arange(10), drange(2000, 9)), 'b'), np.array([26090, 26090, 26090, 26091, 26092, 26093, 26094, 26095, 26095, 26095]))
    >>> assert eq(clock(pd.Series(np.arange(10), drange(2000, '9b', '1b')), 'b'), np.arange(26090, 26100))

    >>> assert eq(clock(np.arange(10)), np.arange(1,11))
    >>> assert eq(clock(pd.Series(np.arange(10)), t = 5), np.arange(6,16))
    >>> assert eq(clock(np.arange(10), 'i'), np.arange(1,11))

    """
    if t is None or is_nan(t):
        t = 0
    if is_str(time):
        time = time[0].lower()
    if is_ts(time) or is_arr(time):
        return time
    elif time is None and (is_arr(ts) or is_pd(ts)):
        return np.full(len(ts), np.nan)
        # mask = np.min(np.isnan(ts), axis = 1) if len(ts.shape) == 2 else np.isnan(ts)
        # res = np.full(len(ts), 1)
        # res[mask] = 0
        # return t + np.cumsum(res)        
    elif time == 'i' and (is_arr(ts) or is_pd(ts)): #index
        return t + np.arange(1,1+len(ts))
    elif is_ts(ts):    
        if isinstance(time, Calendar):
            return np.vectorize(time.clock)(ts.index)
        elif time == 'b':
            time = calendar()
            return np.vectorize(time.clock)(ts.index)
        elif time in _clocks:
            return _clocks[time](ts.index)
    else:
        raise ValueError('cannot determine time for ts=%s, time=%s'%(ts, time))

