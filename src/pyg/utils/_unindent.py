from pyg.base._loop import loop
from pyg.base._types import is_str, is_float, is_strs
from pyg.base._as_list import as_list, is_rng
from collections import Counter
import re


__all__ = ['unindent']

    
_leading_space = re.compile('^[ ]*')
def _indent(text):
    srch = _leading_space.search(text)
    return 0 if srch is None else len(srch.group(0))

_indent('hi')

def unindent(text, loc = True):
    """
    Example:
    >>> text = '''
    book
        preamble
        chapter 1
            theorem 1
                statement
                proof
            theorem 2
                statement
                proof
        chapter 2
            theorem 3
                statement
                proof
            theorem 4
                statement
                proof
        conclusion
        '''
    >>> unindent(text)

    [{0: ''},
     {0: 'book'},
     {0: 'book', 1: 'preamble'},
     {0: 'book', 1: 'chapter 1'},
     {0: 'book', 1: 'chapter 1', 2: 'theorem 1'},
     {0: 'book', 1: 'chapter 1', 2: 'theorem 1', 3: 'statement'},
     {0: 'book', 1: 'chapter 1', 2: 'theorem 1', 3: 'proof'},
     {0: 'book', 1: 'chapter 1', 2: 'theorem 2'},
     {0: 'book', 1: 'chapter 1', 2: 'theorem 2', 3: 'statement'},
     {0: 'book', 1: 'chapter 1', 2: 'theorem 2', 3: 'proof'},
     {0: 'book', 1: 'chapter 2'},
     {0: 'book', 1: 'chapter 2', 2: 'theorem 3'},
     {0: 'book', 1: 'chapter 2', 2: 'theorem 3', 3: 'statement'},
     {0: 'book', 1: 'chapter 2', 2: 'theorem 3', 3: 'proof'},
     {0: 'book', 1: 'chapter 2', 2: 'theorem 4'},
     {0: 'book', 1: 'chapter 2', 2: 'theorem 4', 3: 'statement'},
     {0: 'book', 1: 'chapter 2', 2: 'theorem 4', 3: 'proof'},
     {0: 'book', 1: 'conclusion'},
     {0: 'book', 1: ''}]
    
    Parameters
    ----------
    text : TYPE
        DESCRIPTION.

    Returns
    -------
    text : TYPE
        DESCRIPTION.

    """
    if is_str(text):
        txt = text.split('\n')
    elif is_strs(text):
        txt = text
    else:
        return text
    indents = {}
    res = []
    for t in txt:
        i = _indent(t)
        indents = {k: v for k, v in indents.items() if k<i}
        indents[i] = t[i:]
        if loc:
            row = sorted(indents.items())
            res.append({k : row[k][1] for k in range(len(row))})
        else:
            res.append(indents)
    return res
        
