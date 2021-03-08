.. pyg documentation master file, created by
   sphinx-quickstart on Thu Feb  4 12:43:24 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

****************
Welcome to pyg!
****************

- If you examine data by multiple dimensions, you need pyg.base.dictable.
- If you use MongoDB, you need pyg.mongo. 
- If you use pandas for timeseries analysis, you should consider using pyg.timeseries. 

pyg is both succinct and powerful and makes your code almost boilerplate free and easy to maintain.
As an example, I estimate that Man AHL, a leading quant hedge fund, relies on about 50 coders to replicate the functionality and maintain boilerplate code that pyg would make redundant. 

Below is autodoc created by sphinx followed by tutorials created in jupyter notebooks.

.. toctree::
   :maxdepth: 3

   base
   mongo
   timeseries
   tutorials
   lab/tutorial_dict
   lab/tutorial_dictable
   lab/tutorial_mongo
   lab/tutorial_cell
   lab/tutorial_perdictable
   lab/tutorial_timeseries
   lab/tutorial_ts_decorators
   lab/tutorial_ts_ewma_time

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

