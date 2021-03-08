from pyg.base._loop import loop
from pyg.base._types import is_str, is_float
from pyg.base._as_list import as_list, is_rng

alphabet = 'abcdefghijklmnopqrstuvwxyz'
ALPHABET = alphabet.upper()

__all__ = ['alphabet', 'ALPHABET', 'f12', 'replace', 'lower', 'upper', 'proper', 'strip', 'split', 'capitalize', 'common_prefix', 'deprefix']


@loop(list, dict, tuple)
def _f12(value):
    if is_float(value):
        return '%1.2f'%value
    else:
        return value

@loop(list, dict, tuple)
def _lower(text):
    return text.lower() if is_str(text) else text

@loop(list, dict, tuple)
def _upper(text):
    return text.upper() if is_str(text) else text


@loop(list, dict, tuple)
def _capitalize(text):
    return text.capitalize() if is_str(text) else text

_bbg = { k: ' ' + k.upper() for k in ['Comdty', 'Index', 'Curncy', 'Corp', 'Pfd', 'Equity', 'Govt', 'Mtge']}

@loop(list, dict, tuple)
def _bbgcase(text):
    if is_str(text):
        text = text.upper()
        for k, kupper  in _bbg.items():
            if text.endswith(kupper):
                return text.replace(kupper, ' ' + k)
            elif kupper + ' ' in text:
                return text.replace(kupper, ' ' + k)
    return text

@loop(list, dict, tuple)
def _proper(text):
    if is_str(text):
        return ' '.join([t.capitalize() for t in text.split(' ')])
    return text    
    
@loop(list, dict, tuple)
def _replace(text, old, new = None):
    if is_str(text):
        new = new or ''
        for arg in as_list(old):
            if arg in new:
                raise ValueError('cannot replace indefinitely "%s" with "%s"'%(arg, new))
            while arg in text:
                text = text.replace(arg, new)
    return text

@loop(list, dict, tuple)
def _strip(text, chars = None):
    if is_str(text):
        return text.strip(chars)
    return text


def replace(text, old, new = None):
    """
    A souped up version of text.replace(old, new)
    
    :Example: replace continues to replace until no-more is found
    --------------------------------------------------------------
    >>> assert replace('this    has lots  of   double    spaces', ' '*2, ' ') == 'this has lots of double spaces'
    >>> assert replace('this, sentence? has! too, many, punctuations!', list(',?!.')) == 'this sentence has too many punctuations'
    >>> assert replace(dict(a = 1, b = [' text within a list ', 'and within a dict']), ' ') == {'a': 1, 'b': ['textwithinalist', 'andwithinadict']}
    """
    return _replace(text, old = old, new = new)

def common_prefix(*values):
    """
    
    :Parameters:
    ----------------
    *values : list of iterables
        values for which we want to find common prefix

    :Returns:
    -------
    iterable
        the common prefix.
        
    :Example:
    --------------
    >>> assert common_prefix(['abra', 'abba', 'abacus']) == 'ab'
    >>> assert common_prefix('abra', 'abba', 'abacus') == 'ab'
    >>> assert common_prefix() is None
    >>> assert common_prefix([1,2,3,4], [1,2,3,5,8]) == [1,2,3]

    """
    values = as_list(values)
    if len(values) == 0:
        return None    
    def _all_match(i):
        i0 = values[0][i]
        for v in values:
            if v[i]!=i0:
                return False
        return True
    n = min([len(v) for v in values])
    i = 0
    while i<n and _all_match(i):
        i = i + 1
    return values[0][:i]

def deprefix(*values, sep = None):
    """
    remnoves the common prefix. If a sep is provided then remove only 'whole words' 
    

    :Parameters:
    ----------
    *values : TYPE
        list of values to be de-prefixed.
    sep : str, optional
        separations of wards in text. The default is None.

    :Returns:
    -------
    list
        de-prefixed values.

    :Example:
    ---------
    >>> assert deprefix('abra', 'abba') == ['ra', 'ba']
    >>> assert deprefix('abra', 'abba', sep ='_') == ['abra', 'abba']
    >>> assert deprefix('lhs_a', 'lhs_b', sep ='_') == ['a', 'b']
    
    """
    values = as_list(values)
    if sep is None:
        n = len(common_prefix(values))
        return [v[n:] for v in values]
    else:
        splitted = [v.split(sep) for v in values]
        res = deprefix(*splitted)
        return [sep.join(v) for v in res]
        

@loop(list, dict, tuple)
def _split(text, sep = ' ', dedup = False):
    if is_str(text):
        if is_rng(sep):
            if len(sep) == 0:
                sep = ' '
            elif len(sep) == 1:
                sep = sep[0]
            else:
                text = _replace(text, sep[1:], sep[0])
                sep = sep[0]
        res = text.split(sep)
        if dedup:
            res = [word for word in res if word]
        return res
    else:
        return text


def split(text, sep = ' ', dedup = False):
    """
    equivalent to txt.split(sep) but supporsts:
        - does not throw on non-string
        - removal of multiple seps
        - ensuring there is a unique single separator

    :Parameters:
    ----------
    text : str
        text to be stipped.
    sep : str, list of str, optional
        text used to strip. The default is ' '.
    dedup : bool, optional
        If True, will remove duplicated instances of seps. The default is False.

    :Returns:
    -------
    str
        splitted text
        
    :Example:
    ---------
    >>> text = '   The quick... brown .. fox... '    
    >>> assert split(text) == ['', '', '', 'The', 'quick...', 'brown', '..', 'fox...', '']
    >>> assert split(text, [' ', '.'], True) == ['The', 'quick', 'brown', 'fox']
    >>> text = dict(a = 'Can split this', b = '..and split this too')
    >>> assert split(text, [' ', '.'], True) == {'a': ['Can', 'split', 'this'], 'b': ['and', 'split', 'this', 'too']}
    """
    return _split(text, sep = sep, dedup = dedup)

def f12(value):
    return _f12(value)

def lower(value):
    """
    equivalent to txt.lower() but:
        - does not throw on non-string
        - supports lists/dicts
        
    :Example:
    ---------
    >>> assert lower(['The Brown Fox',1]) == ['the brown fox',1]
    >>> assert lower(dict(a = 'The Brown Fox', b = 3.0)) ==  {'a': 'the brown fox', 'b': 3.0}
    """
    return _lower(value)

def upper(value):
    """
    equivalent to txt.upper() but:
        - does not throw on non-string
        - supports lists/dicts
        
    :Example:
    ---------
    >>> assert upper(['The Brown Fox',1]) == ['THE BROWN FOX',1]
    >>> assert upper(dict(a = 'The Brown Fox', b = 3.0)) ==  {'a': 'THE BROWN FOX', 'b': 3.0}
    """
    return _upper(value)

def proper(value):
    """
    equivalent to Excel's PROPER(txt) but:
        - does not throw on non-string
        - supports lists/dicts
        
    :Example:
    ---------
    >>> assert proper(['THE BROWN FOX',1]) == ['The Brown Fox',1]
    >>> assert proper(dict(a = 'THE BROWN FOX', b = 3.0)) ==  {'a': 'The Brown Fox', 'b': 3.0}
    """
    return _proper(value)

def strip(value):
    """
    equivalent to txt.strip() but:
        - does not throw on non-string
        - supports lists/dicts
        
    :Example:
    ---------
    >>> assert strip([' whatever you say  ','  whatever you do..   ']) == ['whatever you say', 'whatever you do..']
    >>> assert strip(dict(a = ' whatever you say  ', b = 3.0)) ==  {'a': 'whatever you say', 'b': 3.0}
    """
    return _strip(value)

def bbgcase(value):
    return _bbgcase(value)

def capitalize(value):
    """
    equivalent to text.capitalize() but:
        - does not throw on non-string
        - supports lists/dicts
        
    :Example:
    ---------
    >>> assert capitalize('alan howard') == 'Alan howard' # use proper to get Alan Howard
    >>> assert capitalize(['alan howard', 'donald trump']) == ['Alan howard', 'Donald trump'] # use proper?
    """
    return _capitalize(value)
    