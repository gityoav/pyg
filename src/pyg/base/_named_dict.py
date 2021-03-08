from operator import itemgetter as _itemgetter
import sys as _sys
from pyg.base._as_list import as_list

__all__ = ['named_dict']

def _as_str(types):
    return '{%s}'%(', '.join(["'%s' : %s"%(k, str(v).split('.')[-1].replace("'", '')) for k, v in types.items()]))

def _import_value(v):
    # if isinstance(v, (list, tuple)):
    #     return sum([_import_value(w) for w in v], [])
    if isinstance(v, dict):
        return sum([_import_value(w) for w in v.values()], [])
    else:
        if '.' in v:
            vs = v.split('.')
            return ['from %s import %s'%('.'.join(vs[:-1]), vs[-1])]
        else:
            return []
    
def _as_import(types):
    return '\n'.join(set(_import_value(types)))

# def _no_dots(v):
#     if isinstance(v, (list, tuple)):
#         return [_no_dots(i) for i in v]
#     elif isinstance(v, dict):
#         return {key : _no_dots(value) for key, value in v.items()}
#     else:
#         return v.split('.')[-1]

_class_template = '''\
{types_imports}
{casts_imports}
{dict_imports}
class {typename}({basedict}):
   """
   A {typename}({keys} with defaults {defaults})
   """
   _defaults = {defaults}
   _types = {types}
   _keys = {keys}
   _casts = {casts}
   
   def __init__(self, *args, **kwargs):
       if len(args) == 1 and len(kwargs) == 0 and isinstance(args[0], dict):
           kwargs = args[0]
           args = ()
       elif len(args)>0:
           kwargs.update(dict(zip(self._keys, args)))
           args = ()
{defaults_check}
       super({typename}, self).__init__(**kwargs)
       missing_keys = set(self._keys) - set(self.keys())
       if len(missing_keys) > 0:
           raise ValueError('must also provide %s'%missing_keys)
{casts_check}
{types_check}
   def __repr__(self):
       return '{typename}%s'%super({typename}, self).__repr__()
'''

_defaults_check = '''\
       for key in self._defaults:
           if key not in kwargs:
               kwargs[key] = self._defaults[key]
'''

_types_check = '''\
       for key, value in self._types.items():
           if not isinstance(value, type) and callable(value):
               if not value(self[key]):
                   raise ValueError('parameter %s failded %s(%s) validation'%(key, value, self[key]))
           elif not isinstance(self[key], value):
               raise TypeError('parameter %s = %s must be of type %s'%(key, self[key], value))
'''

_casts_check = '''\
       for key, value in self._casts.items():
           self[key] = value(self[key])
'''

def named_dict(name, keys, defaults = {}, types = {}, casts = {}, basedict = 'pyg.base.dictattr', debug = False):
    '''
    This forms a base for all classes. It is similar to named_tuple but:
    
    - supports additional features such as casting/type checking. 
    - support default values
    
    The resulting class is a dict so can be stored in MongoDB, sent to json or be used to construct a pd.Series automatically.
    
    :Example: Simple construction
    ------------------------------
    >>> Customer = named_dict('Customer', ['name', 'date', 'balance'])
    >>> james = Customer('james', 'today', 10)
    >>> assert james['balance'] == 10
    >>> assert james.date == 'today'

    :Example: How named_dict works with json/pandas/other named_dicts 
    -----------------------------------------------------------------
    >>> class Customer(named_dict('Customer', ['name', 'date', 'balance'])):
    >>>     def add_to_balance(self, value):
    >>>         res = self.copy()
    >>>         res.balance += value
    >>>         return res

    >>> james = Customer('james', 'date', 10)    
    >>> assert james.add_to_balance(10).balance == 20
    >>> import json
    >>> assert pd.Series(james).date == 'date'
    >>> assert dict(james) == {'name': 'james', 'date': 'date', 'balance': 10}
    >>> assert json.dumps(james) == '{"name": "james", "date": "date", "balance": 10}'

    >>> class VIP(named_dict('VIP', ['name', 'date'])):
    >>>     def some_method(self):
    >>>         return 'inheritence between classes works as long as members can share'
    
    >>> vip = VIP(james)
    >>> assert vip.name == 'james' ## members moved seemlessly
    >>> assert vip.some_method() == 'inheritence between classes works as long as members can share' 

    :Example: Adding defaults 
    -------
    >>> Customer = named_dict('Customer', ['name', 'date', 'balance'], defaults = dict(balance = 0))
    >>> james = Customer('james', 'today')
    >>> assert james['balance'] == 0

    :Example: types checking
    -------
    >>> import datetime
    >>> Customer = named_dict('Customer', ['name', 'date', 'balance'], defaults = dict(balance = 0), types = dict(date = 'datetime.datetime'))
    >>> james = Customer('james', datetime.datetime.now())
    >>> assert james['balance'] == 0

    :Example: casting
    ----------------------------
    >>> Customer = named_dict('Customer', ['name', 'date', 'balance'], defaults = dict(balance = 0), types = dict(date = 'datetime.datetime'), casts = dict(balance = 'float'))
    >>> james = Customer('james', datetime.datetime.now(), balance = '10.3')
    >>> assert james['balance'] == 10.3


    :Parameters:
    ----------------
    name : str
        name of new class.
    keys : list
        list of keys that the class must have as members.
    defaults : dict, optional
        default values for the keys. The default is {}.
    types : type or callable, optional
        A test to be applied for keys either as a callable or as a type. The default is {}.
    casts : dict, optional
        function. The default is {}.
    basedict : str, optional
        name of the dict class to inherit from. The default is 'dict'.
    debug : bool, optional
        output the construction text if set to True. The default is False.

    Raises
    ------
    ValueError
        DESCRIPTION.

    :Returns:
    -------
    result : new class that inherits from a dict

    '''
    
    
    
    keys = as_list(keys)
    typename = name
    types_check = '' if len(types) == 0 else _types_check
    casts_check = '' if len(casts) == 0 else _casts_check
    defaults_check = '' if len(defaults) == 0 else _defaults_check
    
    if len(defaults) and sorted(defaults.keys()) != sorted(keys[-len(defaults):]):
        raise ValueError('default parmamters %s must come last in %s'%(defaults, keys))
    types_imports = _as_import(types)
    casts_imports = _as_import(casts)
    dict_imports = _as_import(basedict)
    
    class_definition = _class_template.format(keys = keys, 
                                              basedict = basedict.split('.')[-1], 
                                              defaults = defaults, 
                                              typename = typename, 
                                              casts = _as_str(casts), 
                                              types = _as_str(types), 
                                              types_imports = types_imports,
                                              casts_imports = casts_imports, 
                                              dict_imports = dict_imports,
                                              casts_check = casts_check,
                                              types_check = types_check,
                                              defaults_check = defaults_check)
    if debug:
        print(class_definition)
    namespace = dict(_itemgetter = _itemgetter, _property = property, basedict = basedict)
    exec(class_definition, namespace)
    result = namespace[typename]
    result.__module__ = _sys._getframe(1).f_globals.get('__name__', '__main__')
    return result
                                              
                                              
                                              
                                              
                                              
                    

