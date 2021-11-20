README
******
pyg is virtually all you need for an industry-grade systematic hedge fund foundation layer out of the box. 
pyg is both succinct and powerful and makes your code almost boilerplate free and easy to maintain.
I estimate that Man AHL, a leading quant hedge fund, relies on about 50 coders to replicate the functionality and maintain boilerplate code that pyg would make redundant. 

It has several distinct components:

- If you examine data by multiple dimensions, you need pyg.base.dictable and pyg.base.Dict.
- If you use MongoDB, you need pyg.mongo. We provide a TinyDB-like sync/async interface over MongoDB
- If you use pandas for timeseries analysis, you should consider using pyg.timeseries. 
- We also implement a DAG (directed acyclic graph) frameworks using cell/acell for sync/async in-memory graphs and db_cell/db_acell for sync/async graphs with MongoDB persistence.

Below is autodoc created by sphinx followed by tutorials created in jupyter notebooks.
