README
******

- If you examine data by multiple dimensions, you need pyg.base.dictable and pyg.base.Dict.
- If you use MongoDB, you need pyg.mongo. 
- If you use pandas for timeseries analysis, you should consider using pyg.timeseries. 
- If you have a DAG (directed acyclic graph) framework for your calculations, then check out pyg.base.cell and pyg.mongo.db_cell. This supports both pull & push (callback) approach to graph calculation

pyg is both succinct and powerful and makes your code almost boilerplate free and easy to maintain.
I estimate that Man AHL, a leading quant hedge fund, relies on about 50 coders to replicate the functionality and maintain boilerplate code that pyg would make redundant. 

Below is autodoc created by sphinx followed by tutorials created in jupyter notebooks.
