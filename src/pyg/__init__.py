import pandas as pd; import numpy as np
from functools import partial, reduce

try: 
    from pyg.npy import *
except Exception:
    print('pyg_npy not imported')

try:
    from pyg.base import *
    path = cfg_read().get('PYTHONPATH')
    if path:
        import sys
        for p in as_list(path)[::-1]:
            if p not in sys.path:
                logger.info('added %s to sys.path'%p)
                sys.path.insert(0, p)

except Exception:
    print('pyg_base not imported')

try:
    ignore = cfg_read().get('pyg_ignore', [])
except Exception:
    ignore = []

if 'pyg_bond' not in ignore:
    try:
        from pyg.bond import * 
    except Exception:
        print('pyg_bond not imported')

if 'pyg_encoders' not in ignore:
    try:
        from pyg.encoders import * 
    except Exception:
        print('pyg_encoders not imported')

if 'pyg_sql' not in ignore:
    try: 
        from pyg.sql import *
    except Exception:
        print('pyg_sql not imported')

if 'pyg_mongo' not in ignore:
    try: 
        from pyg.mongo import *
    except Exception:
        print('pyg_mongo not imported')

if 'pyg_cell' not in ignore:
    try: 
        from pyg.cells import *
    except Exception:
        print('pyg_cell not imported')

if 'pyg_timeseries' not in ignore:
    try: 
        from pyg.timeseries import *
    except Exception:
        print('pyg_timeseries not imported')
