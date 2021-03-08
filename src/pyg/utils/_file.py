import os
import shutil
import re
from pyg.base._logger import logger
from pyg.base import path_dirname, mkdir, path_join
import subprocess


def _as_check(check):
    if check is None:
        return lambda value: True
    if isinstance(check, str):
        check = re.compile(check)
    if isinstance(check, re.Pattern):
        return lambda value, check = check : check.search(value) is not None
    else:
        return check
    
    
def _add_suffix(value, suffix):
    values = value.split('.')
    return '.'.join(values[:-1]) + suffix + '.%s'%values[-1]
    
def _as_relabel(relabel):
    if relabel is None:
        return lambda value: value
    if isinstance(relabel, str):
        if relabel[0] in ('.', '_'):
            return lambda value, suffix = relabel: _add_suffix(value, suffix) 
        else:
            return lambda value, prefix = relabel: relabel + value
    else:
        return relabel
        

def mvdir(source, target = None, relabel = None, check = None):
    """
    moves files from source to target directory.

    Parameters
    ----------
    source : str
        source path.
    target : str, optional
        taget path. The default is None = current one.
    relabel : callable, or a suffix string, optional
        a method to rename the files we move, The default is None which does not change file name
    check : callable, optional
        a function to check which files should be moved. The default is None.

    """
    src = path_dirname(source)
    tgt = mkdir(target)
    if not os.path.exists(src):
        logger.warning('WARN: no path %s found'%src)
        return None
    files = os.listdir(src)
    if check is not None:
        check = _as_check(check)
        files = [f for f in files if check(f)]
    rename = _as_relabel(relabel)
    for f in files:
        fname = rename(f)
        shutil.move(path_join(src, f), path_join(tgt, fname))
    return

def cmd(command):
    """
    Launches a subprocess and run command on 'cmd window'
    """
    logger.info('INFO: running an external application: {}'.format(command))
    proc = subprocess.Popen(command)
    output, errors = proc.communicate()
    if proc.returncode!=0:
        raise ValueError('{} returned: \nstdout:{} \nstderr:{}'.format(command, output, errors))
    else:
        logger.info('INFO: returned: \nstdout: {} \nstderr:{}'.format(output, errors))
    return output, errors

def listsubdir(path):
    """
    Equivalent to os.listdir(path) except it is recursive within all subdirs 
    """
    fnames = sorted(os.listdir(path))
    res = []
    for f in fnames:
        pf = os.path.join(path, f)
        if os.path.isdir(pf):
            res = res + listsubdir(pf)
        else:
            res.append(pf.replace('\\', '/'))
    return res
# -*- coding: utf-8 -*-

