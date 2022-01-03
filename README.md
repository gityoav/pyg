# pyg

Python tools to handle 

- fast data management using dictable, 
- simplified mongodb access both sync/async and supporting seemless push/pull of complete objects and pandas dataframes
- timeseries analytics that work the same across pandas and numpy.

A conda installation is available from my anaconda channel https://anaconda.org/yoavgit/pyg

- Full manual pdf is available here https://github.com/gityoav/pyg/blob/master/docs/pyg.pdf
- HTML manual is available here: https://gityoav.github.io/pygio/
- Interactive tutorials using jupyter and can be found in docs/lab

pyg is now split into separate packages:
- pyg-base: fast data management and DAG creation 
- pyg-mongo: synchronous mongodb access
- pyg-timeseries: timeseries analytics
- pyg-mongo-async: extending pyg-mongo to asynchronous mongo access using the Motor package. 
