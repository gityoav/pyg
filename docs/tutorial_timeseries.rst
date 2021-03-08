tutorial: pyg.timeseries 
************************

Given pandas, why do we need another timeseries library? 
Indeed, pyg.timeseries 

- contains very few functions that offer any new functionality above what is available through pandas.
- for data with no nan, pyg.timeseries and pandas match exactly. 

So why? Although pandas is amazing, there are a few features in pyg.timeseries designed to address these issues that are important to us:

- pyg.timeseries works seemlessly on pandas objects and on numpy arrays with no code change. 
- pyg.timeseries handles nan consistently across all its functions. For example, for pandas:
    - a.expanding() & a.ewm() **ignore** nan's for calculation and then ffill the result.
    - a.diff(), a.rolling() **include** any nans in the calculation, leading to nan proliferation.
- pg.timeseries exposes the state of the internal functions. The exposure of internal states allows the speeding up of two very common problems in finance:
    - risk calculations, Monte Carlo scenarios: We can run a trading strategy up to today and then generate multiple scenarios and see what-if, without having to rerun the full history. 
    - live versus history: pandas is designed to run a full historical simulation. However, once we reach "today", speed is of the essense and running a full historical simulation every time we ingest a new price, is just too slow. That is why most fast trading is built around fast state-machines. pyg gives you the ability to run two near-identical systems in parallel with almost the same code base: run history overnight and then run today's code base instantly.

Performance-wise, pyg is based around just-in-time compiled functions from numba. When run on pd.Series and pd.DataFrame, pyg is broadly equivalent to pandas and when run on numpy arrays, pyg is considerably faster.

Using pyg.timeseries to handle nan
==================================
One of our main issues with pandas is how it handles nan within rolling, diff, and shift functionality.

We first create a fake timeseries with data and dates over some fake holiday...

>>> from pyg import *; import pandas as pd; import numpy as np
>>> cal = calendar('dummy', holidays = [dt(2000,1,6), dt(2000,1,12)])
>>> a = pd.Series(np.arange(10) * 1., cal.drange(2000, '10b', '1b'))

Now let us assume we want to align this timeseries with other data...

>>> dates = drange(2000,20,'1b')
>>> b = a.reindex(dates)
>>> b
>>> 2000-01-03    0.0
>>> 2000-01-04    1.0
>>> 2000-01-05    2.0
>>> 2000-01-06    NaN  # < -- holiday
>>> 2000-01-07    3.0
>>> 2000-01-10    4.0
>>> 2000-01-11    5.0
>>> 2000-01-12    NaN
>>> 2000-01-13    6.0
>>> 2000-01-14    7.0
>>> 2000-01-17    8.0
>>> 2000-01-18    9.0
>>> 2000-01-19    NaN
>>> 2000-01-20    NaN
>>> 2000-01-21    NaN
>>> dtype: float64

Pandas approach to nan
----------------------
When we have a historic timeseries with nan, a nan is associated with a date for a good reason: perhaps the market was not trading? 
The pandas shift() operator will move the nan to a different date.

>>> pd.concat([b, b.shift()], axis=1)
>>>               0    1
>>> 2000-01-03  0.0  NaN
>>> 2000-01-04  1.0  0.0
>>> 2000-01-05  2.0  1.0
>>> 2000-01-06  NaN  2.0 # < how can you have a reading here? The market was closed!
>>> 2000-01-07  3.0  NaN # < suddenly a nan here...
>>> 2000-01-10  4.0  3.0

From a conceptual perspective, we find this wrong. It also means that when we come to ask questions about the trading system, if b has nans, b.shift() is likely not to be quite what we need. 
Similarly, let us calculate the 1-day of difference of b, we notice a.diff().reindex() is NOT the same as a.reindex().diff()

>>> pd.concat([a.diff().reindex(dates), a.reindex(dates).diff()], axis=1)
>>>               0    1
>>> 2000-01-03  NaN  NaN
>>> 2000-01-04  1.0  1.0
>>> 2000-01-05  1.0  1.0
>>> 2000-01-06  NaN  NaN
>>> 2000-01-07  1.0  NaN # <---- new nan's appear suddenly..

You may think that summing elements and taking their diff are commutative and can be interchanged, but no... diff().cumsum() and cumsum().diff() are very distinct:

>>> pd.concat([b.cumsum().diff(), b.diff().cumsum()], axis=1)
>>>               0    1
>>> 2000-01-03  NaN  NaN
>>> 2000-01-04  1.0  1.0
>>> 2000-01-05  2.0  2.0
>>> 2000-01-06  NaN  NaN
>>> 2000-01-07  NaN  NaN
>>> 2000-01-10  4.0  3.0
>>> 2000-01-11  5.0  4.0
>>> 2000-01-12  NaN  NaN
>>> 2000-01-13  NaN  NaN
>>> 2000-01-14  7.0  5.0
>>> 2000-01-17  8.0  6.0
>>> 2000-01-18  9.0  7.0  ##<--- b.diff() proliferated two extra nan so now it sums up to 7 rather than 9
>>> 2000-01-19  NaN  NaN
>>> 2000-01-20  NaN  NaN
>>> 2000-01-21  NaN  NaN

We find this logical trap to lead to many actual errors in code. To avoid that, one ends up forward filling at every opportunity, though forward-filling comes with its own problems.

pyg approach to nan
--------------------
Whenever we see nan, we assume we can skip it, it probably was created as a result of reindexing but we assume it is irrelevant to the true underlying data. 

- unless explicitly filled/interpolated, any timeseries operation will leave a nan where a nan existed previously.
- nan is excluded from any calculation and arithmetic so introducing nans (e.g. via resample) does not affect final outcome.
- the dates where nan occur will not change. even under the shift operator.

:Example: shift & reindex
-------------------------

>>> assert eq(shift(a.reindex(dates)), shift(a).reindex(dates))
>>> pd.concat([shift(a.reindex(dates)), shift(a).reindex(dates)], axis=1)
>>>               0    1
>>> 2000-01-03  NaN  NaN
>>> 2000-01-04  0.0  0.0
>>> 2000-01-05  1.0  1.0
>>> 2000-01-06  NaN  NaN
>>> 2000-01-07  2.0  2.0
>>> 2000-01-10  3.0  3.0

:Example: diff & reindex
-------------------------

>>> assert eq(diff(a.reindex(dates)), diff(a).reindex(dates))
>>> pd.concat([diff(a.reindex(dates)), diff(a).reindex(dates)], axis=1)
>>>               0    1
>>> 2000-01-03  NaN  NaN
>>> 2000-01-04  1.0  1.0
>>> 2000-01-05  1.0  1.0
>>> 2000-01-06  NaN  NaN
>>> 2000-01-07  1.0  1.0 # <--- no new nan

:Example: diff & cumsum
-------------------------

>>> assert eq(cumsum(diff(b)), diff(cumsum(b)))
>>> pd.concat([cumsum(diff(b)), diff(cumsum(b))], axis=1)
>>>               0    1
>>> 2000-01-03  NaN  NaN
>>> 2000-01-04  1.0  1.0
>>> 2000-01-05  2.0  2.0
>>> 2000-01-06  NaN  NaN
>>> 2000-01-07  3.0  3.0
>>> 2000-01-10  4.0  4.0
>>> 2000-01-11  5.0  5.0


Using pyg.timeseries to manage state
====================================
One of the problem in timeseries analysis is writing research code that works in analysing past data but ideally, the same code can be used in live application. 
One easy approach is "stick the extra data point at the end and run it again from 1980". This leaves us with a single code base but for many live applications (e.g. live trading), this is not viable. 

Further, given our positions today, we may want to run simulations of "what happens next?" to understand what the system is likely to do should various events occur.
Risk calculations are expensive and re-running 10k Monte Carlo scenarios, each time running from 1980 is expensive.

Conversely, we can run research and live systems on two separate code base. This makes live systems responsive but six months down the line, we realise research code base and live code base did not do quite the same thing.

pyg approaches this problem by exposing the internal state of each of its calculation. Each function has two versions:

- function(...) returns the calculation as performed by pandas
- function_(...) returns a dictionary of dict(data = , state = ). The data agrees with function(...) while the state is a dict we can instantiate new calculations with.

>>> from pyg import *
>>> history = pd.Series(np.random.normal(0,1,1000), drange(-1000,-1))
>>> history_signal = ewma_(history, 10) 
>>> assert abs(history_signal.data-history.ewm(10).mean()).max()<1e-10 ### history_signal.data is same as pandas
>>> live = pd.Series(np.random.normal(0,1,10), drange(9))
>>> live_signal = ewma(live, 10, **history_signal)

>>> #### now let us do this by 'sticking the two together'
>>> joint_data = pd.concat([history, live])
>>> joint_signal = ewma(joint_data, 10)
>>> assert eq(live_signal, joint_signal[dt(0):])  # The live signal is the same, even though it only received live data for its calculation.


This allows us to set up two parallel calculations:

+------------------+---------------------+---------------------+------------------+
| workflow         | historic data       | live data           | risk analysis    |    
+==================+=====================+=====================+==================+
| when run?        | research/overnight  | live                | overnight        |
+------------------+---------------------+---------------------+------------------+
| data source?     | ts = long timeseries| a = short ts/array  | 1000's of sims   |
+------------------+---------------------+---------------------+------------------+
| speed?           | long, non-critical  | instantenous        | quick            |
+------------------+---------------------+---------------------+------------------+
| apply f to data  | x_ = f_(ts)         | x = f(a, **x_)      | same as live     |
+------------------+---------------------+---------------------+------------------+
| apply g          | y_ = g_(ts, x_)     | y = g(a, x, **y_)   | same as live     |
+------------------+---------------------+---------------------+------------------+
| final result h   | z_ = h_(ts, x_, y_) | z = h(a, x, y, **z_)| same as live     |
+------------------+---------------------+---------------------+------------------+

Note that for live trading or risk analysis, we tend to switch and run on numpy arrays rather than pandas object. This speeds up the calculations while introduces no code change.
In the example below we explore how to create state-aware, functions within pyg.
The paradigm is that for most functions, function_ will return not just the timeseries output but also the states.

:Example:
---------
Suppose we try to write an ewma crossover function (the difference of two ewma). We want to normalize it by its own volatility.
Traditionally we will write:

>>> def pandas_crossover(a, fast, slow, vol):
>>>     fast_ewma = a.ewm(fast).mean()
>>>     slow_ewma = a.ewm(slow).mean()    
>>>     raw_signal = fast_ewma - slow_ewma
>>>     signal_rms = (raw_signal**2).ewm(vol).mean()**0.5
>>>     signal_rms[signal_rms==0] = np.nan
>>>     normalized = raw_signal/signal_rms
>>>     return normalized

>>> a = pd.Series(np.random.normal(0,1,10000), drange(-9999)); fast = 10; slow = 30; vol = 50
>>> pandas_x = pandas_crossover(a, fast, slow, vol)

We can quickly rewrite it using pyg:

>>> def crossover(a, fast, slow, vol):
>>>     fast_ewma = ewma(a, fast)
>>>     slow_ewma = ewma(a, slow)    
>>>     raw_signal = fast_ewma - slow_ewma
>>>     signal_rms = ewmrms(raw_signal, vol)
>>>     normalized = raw_signal/v2na(signal_rms)
>>>     return normalized
>>> x = crossover(a, fast, slow, vol)
>>> assert abs(x-pandas_x).max()<1e-10

And with very little additional effort, we can write a new function that also exposes the internal state:

>>> _data = 'data'
>>> def crossover_(a, fast, slow, vol, data = None, state = None):
>>>     state = Dict(fast = {}, slow = {}, vol = {}) if state is None else state
>>>     fast_ewma_ = ewma_(a, fast, **state.fast)
>>>     slow_ewma_ = ewma_(a, slow, **state.slow)    
>>>     raw_signal = fast_ewma_.pop(_data) - slow_ewma_.pop(_data)
>>>     signal_rms = ewmrms_(raw_signal, vol, **state.vol)
>>>     normalized = raw_signal/v2na(signal_rms.pop(_data))
>>>     return Dict(data = normalized, state = Dict(fast = fast_ewma_, slow = slow_ewma_, vol = signal_rms))
>>> x_ = crossover_(a, fast, slow, vol)
>>> assert eq(x, x_.data)
  
The three give idential results and we can also verify that crossover_ will allow us to split the evaluation to the long-history and the new data:

>>> history = a[:9900]
>>> live = a[9900:]
>>> x_history = crossover_(history, 10, 30, 50)
>>> x_live = crossover_(live, 10, 30, 50, **x_history)
>>> x_ = crossover_(a, fast, slow, vol)
>>> assert eq(x_live.data , x_.data[9900:])

Have we gained anything?

>>> pandas_old = timer(pandas_crossover, 100)(history, 10, 30, 50)
>>> x_history  = timer(crossover_, 100)(history, 10, 30, 50)
>>> x_live = timer(crossover_, 100)(live.values, 10, 30, 50, **x_history)

>>> TIMER:'pandas_crossover' args:[["<class 'pandas.core.series.Series'>[9900]", '10', '30', '50'], []] (100 runs) took 0:00:00.354514 sec
>>> TIMER:'crossover_' args:[["<class 'pandas.core.series.Series'>[9900]", '10', '30', '50'], []] (100 runs) took 0:00:00.245859 sec
>>> TIMER:'crossover_' args:[["<class 'numpy.ndarray'>[100]", '10', '30', '50']] (100 runs) took 0:00:00.049970 sec

We see that pyg is about 50% faster than pandas. Running just the new data using numpy arrays, is about 5 times faster still. 
Indeed, running 10k 100-day forward scenarios take about 2 seconds at most.

>>> scenarios = np.random.normal(0,1,(100,10000))
>>> x_scenarios = timer(crossover_)(scenarios , 10, 30, 50, **x_history)
>>> TIMER:'crossover_' args:[["<class 'numpy.ndarray'>[100]", '10', '30', '50']] (1 runs) took 0:00:01.943459 sec

Using pyg decorators in constructing timeseries functions
=========================================================
There are a few decorators that are relevant to timeseries analysis

pd2np and compiled
------------------
We write most of our underlying functions assuming the function parameters are 1-d numpy arrays.
If you want them numba.jit compiled, please use the compiled operator.

>>> from pyg import *; import numba
>>> @pd2np
>>> @compiled
>>> def sumsq(a, t2 = 0):
>>>     res = np.empty_like(a)
>>>     for i in range(a.shape[0]):
>>>         if np.isnan(a[i]):
>>>             res[i] = np.nan
>>>         else:
>>>             t2 += a[i]**2
>>>             res[i] = t2
>>>     return res

It is not surpising that sumsq works for arrays. Notice how np.isnan is handled to ensure nans are skipped.

>>> a = np.arange(10)
>>> res_a = sumsq(a)
>>> assert eq(res_a, np.array([  0,   1,   5,  14,  30,  55,  91, 140, 204, 285]))

pd2np will convert a pandas Series to arrays, run the function and convert back to pandas.

>>> s = pd.Series(a, drange(-9))
>>> res_s = sumsq(s)
>>> assert eq(res_s, pd.Series(res_a, drange(-9)))

But this will only work for a 1-dimensional objects, so no df nor 2-d np.ndarray.

loop
-----
We recode the function, this time decorating it with the loop() decorator:

>>> @loop(pd.DataFrame, dict, list, np.ndarray)
>>> @pd2np
>>> @compiled
>>> def sumsq(a, t2 = 0):
>>>     res = np.empty_like(a)
>>>     for i in range(a.shape[0]):
>>>         if np.isnan(a[i]):
>>>             res[i] = np.nan
>>>         else:
>>>             t2 += a[i]**2
>>>             res[i] = t2
>>>     return res

Once we introduce loop, The function will cycle over columns of a DataFrame or a numpy array:

>>> b = a+1
>>> df = pd.DataFrame(dict(a = a, b = b), drange(-9))
>>> ab = df.values  # array
>>> res_df = sumsq(df)
>>> res_ab = sumsq(ab)
>>> assert eq(res_df, pd.DataFrame(dict(a = sumsq(a), b = sumsq(a+1)), drange(-9)))
>>> assert eq(res_df.values, res_ab)

Indeed, since we asked it to loop over dict and list...

>>> assert eq(sumsq([a,b]) , [sumsq(a), sumsq(b)])
>>> assert eq(sumsq(dict(a=a,b=b)) , dict(a=sumsq(a), b=sumsq(b)))

Using pyg to manage indexing and date stamps
============================================

presync
-------
Suppose the function takes two (or more) timeseries. 

>>> @loop(dict, list)
>>> @presync(index = 'inner')
>>> @loop(pd.DataFrame, np.ndarray)
>>> @pd2np
>>> def weighted_mean(a, wgt):
>>>     print(a, wgt)  # We put a print statement so easier to see what's going on...
>>>     aw = 0; w = 0
>>>     for i in range(a.shape[0]):
>>>         if np.isnan(a[i]) or np.isnan(wgt[i]):
>>>             res[i] = np.nan
>>>         else:
>>>             aw += a[i]*wgt[i]
>>>             w += wgt[i]
>>>     return aw/w

>>> a = np.arange(10) * 1.; wgt = a+1
>>> b = a+1
>>> df = pd.DataFrame(dict(a = a, b = b), drange(-9))
>>> assert weighted_mean(a,wgt) == 6
>>> sa = pd.Series(a, drange(-9))
>>> sb = pd.Series(a, drange(-8,1))
>>> swgt = pd.Series(wgt, drange(-8,1))

What happens when the weights and the timeseries are unsynchronized? presync will sync all inputs pre calculations

>>> assert weighted_mean(sa, swgt) == 6+1/3
>>> assert weighted_mean(dict(a = sa, b = sb), swgt) == dict(a = 6+1/3, b = 6)
>>> assert eq(weighted_mean(df, swgt) , pd.Series(dict(a = 6 + 1/3, b = 7 + 1/3)))

If you have not used decorators before, this will look daunthing. Once you get used to it, presync and loop will be indispensible.

:Example: changing presync on the fly
-------------------------------------

>>> @presync
>>> def concat_and_sum(*args):
>>>     args = as_list(args) # this means concat_and_sum(a1,a2,a3) andconcat_and_sum([a1,a2,a3]) work
>>>     args = [pd.DataFrame(a) for a in args]
>>>     return pd.concat(args, axis = 1).sum(axis=1)
>>> a = np.arange(10) * 1.
>>> sa = pd.Series(a, drange(-9))
>>> sb = pd.Series(a, drange(-8,1))

There are only 9 overlapping dates between sa and sb as presync (and concat) by default inner-join on index.

>>> assert len(concat_and_sum(sa,sb)) == 9 

However, concat_and_sum.oj will default to outer-join and 

>>> assert len(concat_and_sum.oj([sa,sb])) == 11  # outer join
>>> assert len(concat_and_sum.lj([sa,sb])) == 10  # left join, use sa index

:Example: presync of numpy arrays.
----------------------------------
When we deal with thousands of equities, one way of speeding calculations is by stacking them all onto huge dataframes. 
This does work but one is always busy fiddling with 'the universe' one supports.

We took a slightly different approach: 

- We define a global timestamp.
- We then sample each timeseries to that global timestamp, dropping the early history where the data is all nan. (df_fillna(ts, index, method = 'fnna')).
- We then do our research on these numpy arrays.
- Finally, once we are done, we resample back to the global timestamp.

While we are in numpy arrays, we can 'inner join' by recognising the 'end' of each array shares the same date.
Indeed df_index, df_reindex and presync all work seemlessly on np.ndarray as well as DataFrames, under that assumption that *the end of all arrays are in sync*.

We find this approach saves on memory and on computation time. It also lends itself to being able to retrieve and create specific universes for specific trading ideas.
It is not without its own issues but that is a separate discussion.

:Example: inner-joining and presync of numpy arrays inputs
----------------------------------------------------------
>>> us = calendar('US')
>>> dates = pd.Index(us.drange('-40y', 0 ,'1b'))

We generate random universe returns of different length:

>>> universe = dictable(stock = ['msft', 'appl', 'tsla'], n = [10000, 8000, 7000])
>>> universe = universe(rtn_ts = lambda n: pd.Series(np.random.normal(0,1,n), us.drange('-%ib'%n, 0, '1b'))[np.random.normal(0,1,n)>-1])

We convert to arrays and then do some basic maths on it:

>>> universe = universe(rtn_ar = lambda rtn_ts: df_reindex(rtn_ts, dates, method = 'fnna').values)
>>> universe = universe(len = lambda rtn_ar: len(rtn_ar))
>>> universe = universe(price = lambda rtn_ar: cumsum(rtn_ar))
>>> universe = universe(vol = lambda rtn_ar: ewmstd(rtn_ar, 30))
>>> print(universe.len)
>>> [10000, 7998, 7000]  # the first returns of appl happened to be nan

Whenever we want, we can inner-join or outer-join specific arrays:

>>> concat = presync(lambda tss: np.array(tss).T)
>>> concat(universe.vol).shape
>>> (7000,3)
>>> concat.oj(universe.vol).shape
>>> (10000,3)

When we are done, we can reindex it back to a pandas DataFrame/Series

>>> np_reindex(concat.oj(universe.vol), index = dates, columns = universe.stock)
>>>                 msft      appl      tsla
>>> 1982-10-13       NaN       NaN       NaN
>>> 1982-10-14       NaN       NaN       NaN
>>> 1982-10-15       NaN       NaN       NaN
>>> 1982-10-18       NaN       NaN       NaN
>>> 1982-10-19       NaN       NaN       NaN
>>>              ...       ...       ...
>>> 2021-02-03  0.985049  0.920324  1.089791
>>> 2021-02-04  0.970382  0.906128  1.073437
>>> 2021-02-05  0.971823       NaN  1.085482
>>> 2021-02-08       NaN  0.891668  1.132042
>>> 2021-02-09  1.033889  0.880831  1.119209
>>> 
>>> [10000 rows x 3 columns]


