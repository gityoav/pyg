import logging

__all__ = ['get_logger', 'logger']

_loggers = dict()

def get_logger(name = 'pyg', level = 'info', fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', file = False, console = True):
    """
    quick utility to simplify loggers creation and ensure we cache them and do not add to many handlers

    :Parameters:
    ----------
    name : str, optional
        name of logger. The default is 'pyg'.
    level : str, optional
        DEBUG/INFO/WARN etc. The default is 'info'.
    fmt : str, optional
        string formatting for messages. The default is '%(asctime)s - %(name)s - %(levelname)s - %(message)s'.
    file : bool/str, optional
        the name of the file to log to. The default is False = do not log to file.
    console : bool, optional
        log to console? The default is True.

    :Returns:
    -------
    logging.logger
    """
    if name in _loggers :
        return _loggers[name]
    logger = logging.getLogger(name)
    level = getattr(logging, level.upper()) if isinstance(level, str) else level
    logger.setLevel(level)
    formatter = logging.Formatter(fmt)
    if console:
        c = logging.StreamHandler()
        c.setLevel(level)
        c.setFormatter(formatter)
        logger.addHandler(c)
    if file:
        f = logging.FileHandler(file)
        f.setLevel(level)
        f.setFormatter(formatter)
        logger.addHandler(f)
    _loggers[name] = logger
    return logger

logger = get_logger()
