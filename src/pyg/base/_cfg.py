import os
import json
CFG = 'c:/etc/pyg.json' # this should really live in PYG_CFG in environment variables

def mkdir(path):
    """
    sames as os.mkdir but 
    1) allows for subdir i.e.:
    2) will not attempt to create if path exits, hence can run safely


    :Example:
    ---------
    >>> with pytest.raises(Exception):       
    >>>     os.mkdir('c:/temp/some/subdir/that/we/want')

    >>> print('but', mkdir('c:/temp/some/subdir/that/we/want'), 'now exists')
    
    """
    if not os.path.isdir(path):
        p = path.replace('\\', '/')
        paths = p.split('/')
        if '.' in paths[-1]:
            paths = paths[:-1]
        for i in range(2, len(paths)+1):
            d = '/'.join(paths[:i])
            if not os.path.isdir(d):
                os.mkdir(d)
    return path

def cfg_read():
    if os.path.isfile(CFG):
        with open(CFG, 'r') as f:
            cfg = json.load(f)
        return cfg
    else:
        return {}
cfg_read.__doc__ = 'reads the config file from %s' % CFG



def cfg_write(cfg):
    with open(mkdir(CFG), 'w') as f:
        json.dump(cfg, f)
cfg_write.__doc__ = 'writes the config file provided to %s' % CFG
    

