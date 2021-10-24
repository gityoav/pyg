****************
pyg.timeseries
****************
Given pandas, why do we need this timeseries library? 
pandas is amazing but there are a few features in pyg.timeseries designed to enhance it. 
There are three issues with pandas that pyg.timeseries tries  to address:

- pandas works on pandas objects (obviously) but not on numpy arrays.
- pandas handles TimeSeries with nan inconsistently across its functions. This makes your results sensitive to reindexing/resampling. E.g.:
    - a.expanding() & a.ewm() **ignore** nan's for calculation and then ffill the result.
    - a.diff(), a.rolling() **include** any nans in the calculation, leading to nan propagation.
- pandas is great if you have the full timeseries. However, if you now want to run the same calculations in a live environment, on recent data, pandas cannot help you: you have to stick the new data at the end of the DataFrame and rerun.

pyg.timeseries tries to address this:

- pyg.timeseries agrees with pandas 100% on DataFrames (with no nan) while being of comparable (if not faster) speed
- pyg.timeseries works seemlessly on pandas objects and on numpy arrays, with no code change. 
- pyg.timeseries handles nan consistently across all its functions, 'ignoring' all nan, making your results consistent regardless of resampling.
- pyg.timeseries exposes the state of the internal function calculation. The exposure of internal states allows us to calculate the output of additional data **without** re-running history. This speeds up of two very common problems in finance:
    - risk calculations, Monte Carlo scenarios: We can run a trading strategy up to today and then generate multiple scenarios and see what-if, without having to rerun the full history. 
    - live versus history: pandas is designed to run a full historical simulation. However, once we reach "today", speed is of the essense and running a full historical simulation every time we ingest a new price, is just too slow. That is why most fast trading is built around fast state-machines. Of course, making sure research & live versions do the same thing is tricky. pyg gives you the ability to run two systems in parallel with almost the same code base: run full history overnight and then run today's code base instantly, instantiated with the output of the historical simulation.


simple functions
=================

diff
----
.. autofunction:: pyg.timeseries._rolling.diff

shift
------
.. autofunction:: pyg.timeseries._rolling.shift

ratio
-----
.. autofunction:: pyg.timeseries._rolling.ratio

ts_count
--------
.. autofunction:: pyg.timeseries._ts.ts_count

ts_sum
--------
.. autofunction:: pyg.timeseries._ts.ts_sum

ts_mean
-------
.. autofunction:: pyg.timeseries._ts.ts_mean

ts_rms
--------
.. autofunction:: pyg.timeseries._ts.ts_rms

ts_std
-------
.. autofunction:: pyg.timeseries._ts.ts_std

ts_skew
--------
.. autofunction:: pyg.timeseries._ts.ts_skew

ts_min
--------
.. autofunction:: pyg.timeseries._ts.ts_max

ts_max
--------
.. autofunction:: pyg.timeseries._ts.ts_max

ts_median
--------
.. autofunction:: pyg.timeseries._ts.ts_median

fnna
----
.. autofunction:: pyg.timeseries._rolling.fnna

v2na/na2v
----------
.. autofunction:: pyg.timeseries._rolling.v2na
.. autofunction:: pyg.timeseries._rolling.na2v

ffill/bfill
------------
.. autofunction:: pyg.timeseries._rolling.ffill
.. autofunction:: pyg.timeseries._rolling.bfill

nona
-----
.. autofunction:: pyg.timeseries._ts.nona


expanding window functions
===========================

expanding_mean
------------------
.. autofunction:: pyg.timeseries._expanding.expanding_mean

expanding_rms
------------------
.. autofunction:: pyg.timeseries._expanding.expanding_rms

expanding_std
------------------
.. autofunction:: pyg.timeseries._expanding.expanding_std

expanding_sum
------------------
.. autofunction:: pyg.timeseries._expanding.expanding_sum

expanding_skew
------------------
.. autofunction:: pyg.timeseries._expanding.expanding_skew

expanding_min
------------------
.. autofunction:: pyg.timeseries._min.expanding_min

expanding_max
------------------
.. autofunction:: pyg.timeseries._max.expanding_max

expanding_median
------------------
.. autofunction:: pyg.timeseries._median.expanding_median

expanding_rank
------------------
.. autofunction:: pyg.timeseries._rank.expanding_rank

cumsum
----------
.. autofunction:: pyg.timeseries._expanding.cumsum

cumprod
----------
.. autofunction:: pyg.timeseries._expanding.cumprod


rolling window functions
=========================
rolling_mean
------------------
.. autofunction:: pyg.timeseries._rolling.rolling_mean

rolling_rms
------------------
.. autofunction:: pyg.timeseries._rolling.rolling_rms

rolling_std
------------------
.. autofunction:: pyg.timeseries._rolling.rolling_std

rolling_sum
------------------
.. autofunction:: pyg.timeseries._rolling.rolling_sum

rolling_skew
------------------
.. autofunction:: pyg.timeseries._rolling.rolling_skew

rolling_min
------------------
.. autofunction:: pyg.timeseries._min.rolling_min

rolling_max
------------------
.. autofunction:: pyg.timeseries._max.rolling_max

rolling_median
----------------
.. autofunction:: pyg.timeseries._median.rolling_median

rolling_quantile
----------------
.. autofunction:: pyg.timeseries._stride.rolling_quantile

rolling_rank
------------------
.. autofunction:: pyg.timeseries._rank.rolling_rank

exponentially weighted moving functions
========================================

ewma
----
.. autofunction:: pyg.timeseries._ewm.ewma

ewmrms
------
.. autofunction:: pyg.timeseries._ewm.ewmrms

ewmstd
------
.. autofunction:: pyg.timeseries._ewm.ewmstd

ewmskew
--------
.. autofunction:: pyg.timeseries._ewm.ewmskew

ewmvar
------
.. autofunction:: pyg.timeseries._ewm.ewmvar

ewmcor
------
.. autofunction:: pyg.timeseries._ewm.ewmcor

ewmcorr
------
.. autofunction:: pyg.timeseries._ewm.ewmcorr

ewmLR
------
.. autofunction:: pyg.timeseries._ewm.ewmLR

ewmGLM
------
.. autofunction:: pyg.timeseries._ewm.ewmGLM

ewmxo
------
.. autofunction:: pyg.timeseries._ewmxo.ewmxo

functions exposing their state
==============================

simple functions
----------------
.. autofunction:: pyg.timeseries._rolling.diff_
.. autofunction:: pyg.timeseries._rolling.shift_
.. autofunction:: pyg.timeseries._rolling.ratio_
.. autofunction:: pyg.timeseries._ts.ts_count_
.. autofunction:: pyg.timeseries._ts.ts_sum_
.. autofunction:: pyg.timeseries._ts.ts_mean_
.. autofunction:: pyg.timeseries._ts.ts_rms_
.. autofunction:: pyg.timeseries._ts.ts_std_
.. autofunction:: pyg.timeseries._ts.ts_skew_
.. autofunction:: pyg.timeseries._ts.ts_max_
.. autofunction:: pyg.timeseries._ts.ts_max_
.. autofunction:: pyg.timeseries._rolling.ffill_

expanding window functions
--------------------------
.. autofunction:: pyg.timeseries._expanding.expanding_mean_
.. autofunction:: pyg.timeseries._expanding.expanding_rms_
.. autofunction:: pyg.timeseries._expanding.expanding_std_
.. autofunction:: pyg.timeseries._expanding.expanding_sum_
.. autofunction:: pyg.timeseries._expanding.expanding_skew_
.. autofunction:: pyg.timeseries._min.expanding_min_
.. autofunction:: pyg.timeseries._max.expanding_max_
.. autofunction:: pyg.timeseries._expanding.cumsum_
.. autofunction:: pyg.timeseries._expanding.cumprod_

rolling window functions
------------------------
.. autofunction:: pyg.timeseries._rolling.rolling_mean_
.. autofunction:: pyg.timeseries._rolling.rolling_rms_
.. autofunction:: pyg.timeseries._rolling.rolling_std_
.. autofunction:: pyg.timeseries._rolling.rolling_sum_
.. autofunction:: pyg.timeseries._rolling.rolling_skew_
.. autofunction:: pyg.timeseries._min.rolling_min_
.. autofunction:: pyg.timeseries._max.rolling_max_
.. autofunction:: pyg.timeseries._median.rolling_median_
.. autofunction:: pyg.timeseries._rank.rolling_rank_
.. autofunction:: pyg.timeseries._stride.rolling_quantile_

exponentially weighted moving functions
---------------------------------------
.. autofunction:: pyg.timeseries._ewm.ewma_
.. autofunction:: pyg.timeseries._ewm.ewmrms_
.. autofunction:: pyg.timeseries._ewm.ewmstd_
.. autofunction:: pyg.timeseries._ewm.ewmvar_
.. autofunction:: pyg.timeseries._ewm.ewmcor_
.. autofunction:: pyg.timeseries._ewm.ewmcorr_
.. autofunction:: pyg.timeseries._ewm.ewmLR_
.. autofunction:: pyg.timeseries._ewm.ewmGLM_
.. autofunction:: pyg.timeseries._ewm.ewmskew_
.. autofunction:: pyg.timeseries._ewm.ewmxo_

Index handling
==============

df_fillna
----------
.. autofunction:: pyg.timeseries._index.df_fillna

df_index
--------
.. autofunction:: pyg.timeseries._index.df_index

df_reindex
----------
.. autofunction:: pyg.timeseries._index.df_reindex

presync
-------
.. autofunction:: pyg.timeseries._index.presync

add/sub/mul/div/pow operators
-----------------------------
.. autofunction:: pyg.timeseries._index.add_
.. autofunction:: pyg.timeseries._index.mul_
.. autofunction:: pyg.timeseries._index.div_
.. autofunction:: pyg.timeseries._index.sub_
.. autofunction:: pyg.timeseries._index.pow_

