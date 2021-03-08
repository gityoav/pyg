from pyg.base import dictable, as_list
import json
row = '### test'
def _markdown2txt(row):
    if '__' in row:
        row = ' '.join([word.replace('__', '**') if word.startswith('__') and word.endswith('__') else word for word in row.split(' ')])
    if row.startswith('###'):
        txt = row[3:].strip()
        return '\n%s\n%s\n'%(txt, '-' * len(txt))
    elif row.startswith('##'):
        txt = row[2:].strip()
        return '\n%s\n%s\n'%(txt, '=' * len(txt))
    elif row.startswith('#'):
        txt = row[1:].strip()
        return '\n%s\n%s\n'%(txt, '*' * len(txt))
    elif row.startswith('<br>'):
        return '\n' + row[4:].strip()
    elif row.startswith('*'):
        return '\n' + row
    else:
        return row


def _output2txt(output_type, text = None, data = None):
    if output_type == 'stream':
        return ''.join(['>>> ' + s for s in as_list(text)])
    elif output_type == 'execute_result':
        return ''.join(['>>> %s'%s for s in as_list(data['text/plain'])]) 

def _code2txt(source, outputs=None):
    res = ''.join(['>>> ' + s for s in as_list(source)])
    outputs = as_list(outputs)
    if len(outputs):
        o = dictable(outputs)
        return ''.join([res, '\n', '\n'] + o[_output2txt] + ['\n'])
    else:
        return res

def _cell2txt(source, outputs, cell_type):
    if cell_type == 'code':
        return _code2txt(source, outputs)
    elif cell_type == 'markdown':
        return ''.join([_markdown2txt(s) for s in as_list(source)])
        
def notebook2rst(notebook, rst):
    """
    If you write your file in a jupyter notebook, the file notebook.ipynb can be converted in sphinx.rst using this. This allows us to write documentation more easily

    >>> notebook2rst('d:/dropbox/yoav/python/pyg/lab/tutorial_dictable.ipynb', 'd:/dropbox/yoav/python/pyg/docs/dictable_tutorial.rst')
    >>> notebook2rst('d:/dropbox/yoav/python/pyg/lab/tutorial_cell.ipynb', 'd:/dropbox/yoav/python/pyg/docs/cell_tutorial.rst')

    Parameters
    ----------
    notebook: str
        Name of a notebook.ipynb file

    rst: str
        Name of a Sphinx notebook.rst file
    
    Returns
    -------
    str
        The rst file string once it has been written.

    """
    with open(notebook, 'r') as f:
        d = f.read()
    j = json.loads(d)
    cells = dictable(j['cells'])
    cells = cells(txt = _cell2txt)
    with open(rst, 'w') as f:
        f.write('\n\n'.join(cells.txt))
    return rst

    