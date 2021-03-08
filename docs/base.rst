*********
pyg.base
*********

extensions to dict
==================


dictattr 
---------
.. autoclass:: pyg.base._dictattr.dictattr
   :members: 
.. autofunction:: pyg.base._dictattr.dictattr.__add__
.. autofunction:: pyg.base._dictattr.dictattr.__sub__
.. autofunction:: pyg.base._dictattr.dictattr.__and__

ulist
-----
The dictattr.keys() method returns a ulist: a list with unique elements:

.. autoclass:: pyg.base._ulist.ulist
   :members: 

Dict
----
.. autoclass:: pyg.base._dict.Dict
   :members: 
.. autofunction:: pyg.base._dict.Dict.__call__

dictable
---------
.. autoclass:: pyg.base._dictable.dictable
   :members: 
.. autofunction:: pyg.base._dictable.dictable.__call__


perdictable
-----------
.. autofunction:: pyg.base._perdictable.perdictable

join
----
.. autofunction:: pyg.base._perdictable.join


named_dict
-----------
.. autofunction:: pyg.base._named_dict.named_dict


decorators
==========

wrapper
-------
.. autoclass:: pyg.base._decorators.wrapper

timer
-----
.. autoclass:: pyg.base._decorators.timer

try_value
--------
.. autofunction:: pyg.base._decorators.try_value

try_back
--------
.. autofunction:: pyg.base._decorators.try_back

loops
------
.. autoclass:: pyg.base._loop.loops

loop
------
.. autofunction:: pyg.base._loop.loop

loop_all is an instance of loops that loops over dict, list, tuple, np.ndarray and pandas.DataFrame/Series

kwargs_support
-------------
.. autofunction:: pyg.base._decorators.kwargs_support

graphs & cells
==============

cell
----
.. autoclass:: pyg.base._cell.cell
   :members: 

cell_go
-------
.. autofunction:: pyg.base._cell.cell_go

cell_item
----------
.. autofunction:: pyg.base._cell.cell_item

cell_func
----------
.. autofunction:: pyg.base._cell.cell_func

encode and decode/save and load
===============================

encode
-------
.. autofunction:: pyg.base._encode.encode

decode
-------
.. autofunction:: pyg.base._encode.decode

pd_to_parquet
--------------
.. autofunction:: pyg.base._parquet.pd_to_parquet

pd_read_parquet
---------------
.. autofunction:: pyg.base._parquet.pd_read_parquet

parquet_encode
---------------
.. autofunction:: pyg.mongo._encoders.parquet_encode


csv_encode
---------------
.. autofunction:: pyg.mongo._encoders.csv_encode


convertors to bytes
------------------
.. autofunction:: pyg.base._encode.pd2bson
.. autofunction:: pyg.base._encode.np2bson
.. autofunction:: pyg.base._encode.bson2np
.. autofunction:: pyg.base._encode.bson2pd



dates and calendar
==================
dt
--
.. autofunction:: pyg.base._dates.dt

ymd
-----
.. autofunction:: pyg.base._dates.ymd

dt_bump
-------
.. autofunction:: pyg.base._dates.dt_bump
drange
------
.. autofunction:: pyg.base._drange.drange

date_range
----------
.. autofunction:: pyg.base._drange.date_range

Calendar
--------
.. autoclass:: pyg.base._drange.Calendar
   :members: 

calendar
--------
.. autofunction:: pyg.base._drange.calendar

as_time
--------
.. autofunction:: pyg.base._drange.as_time


clock
--------
.. autofunction:: pyg.base._drange.clock


text manipulation
=================
lower
------
.. autofunction:: pyg.base._txt.lower

upper
------
.. autofunction:: pyg.base._txt.upper

proper
------
.. autofunction:: pyg.base._txt.proper

capitalize
----------
.. autofunction:: pyg.base._txt.capitalize

strip
----------
.. autofunction:: pyg.base._txt.strip

split
----------
.. autofunction:: pyg.base._txt.split

replace
----------
.. autofunction:: pyg.base._txt.replace

common_prefix
-------------
.. autofunction:: pyg.base._txt.common_prefix


files & directory 
=================

mkdir
-----
.. autofunction:: pyg.base._file.mkdir

read_csv
---------
.. autofunction:: pyg.base._file.read_csv

tree manipulation
=================
Trees are dicts of dicts. just like an item in a dict is (key, value), tree items are just longer tuples: (key1, key2, key3, value)

tree_to_items
-------------
.. autofunction:: pyg.base._dict.tree_to_items

items_to_tree
-------------
.. autofunction:: pyg.base._dict.items_to_tree

tree_update
-----------
.. autofunction:: pyg.base._dict.tree_update

tree_to_table
-------------
.. autofunction:: pyg.base._tree.tree_to_table

tree_repr
-------------
.. autofunction:: pyg.base._tree_repr.tree_repr


list functions
==============
as_list
--------
.. autofunction:: pyg.base._as_list.as_list

as_tuple
--------
.. autofunction:: pyg.base._as_list.as_tuple

first
--------
.. autofunction:: pyg.base._as_list.first

last
--------
.. autofunction:: pyg.base._as_list.last

unique
--------
.. autofunction:: pyg.base._as_list.unique

Comparing and Sorting
=====================
cmp
----
.. autofunction:: pyg.base._sort.cmp
Cmp
---
.. autofunction:: pyg.base._sort.Cmp

sort
----
.. autofunction:: pyg.base._sort.sort

eq
----
.. autofunction:: pyg.base._eq.eq

in_ 
-----
.. autofunction:: pyg.base._eq.in_

bits and pieces
===============
type functions
--------------
.. automodule:: pyg.base._types
   :members: 

zipper
------
.. autofunction:: pyg.base._zip.zipper

reducer
--------
.. autofunction:: pyg.base._reducer.reducer

reducing
--------
.. autoclass:: pyg.base._reducer.reducing

logger and get_logger
---------------------
.. autofunction:: pyg.base._logger.get_logger

access functions
----------------
These are useful to convert object-oriented code to declarative functions

.. automodule:: pyg.base._getitem
   :members: 

inspection
----------
There are a few functions extending the inspect module. 

.. automodule:: pyg.base._inspect
   :members: 


