from pyg.base._txt import alphabet, ALPHABET, f12, replace, lower, upper, proper, strip, split, capitalize, common_prefix, deprefix, bbgcase
import pytest

def test_alphabet():
    assert alphabet == 'abcdefghijklmnopqrstuvwxyz' and len(alphabet) == 26 and sorted(list(alphabet)) == list(alphabet) and len(set(list(alphabet))) == 26

def test_ALPHABET():
    assert ALPHABET == 'abcdefghijklmnopqrstuvwxyz'.upper() and len(alphabet) == 26 and sorted(list(alphabet)) == list(alphabet) and len(set(list(alphabet))) == 26


def test_f12():
    assert f12('hi') == 'hi'
    assert f12(1.3564)== '1.36'

def test_replace():
    assert replace('this    has lots  of   double    spaces', ' '*2, ' ') == 'this has lots of double spaces'
    assert replace('this, sentence? has! too, many, punctuations!', list(',?!.')) == 'this sentence has too many punctuations'
    assert replace(dict(a = 1, b = [' text within a list ', 'and within a dict']), ' ') == {'a': 1, 'b': ['textwithinalist', 'andwithinadict']}
    with pytest.raises(ValueError):
        replace('some random text', '  ', '  ')

def test_split():
    text = '   The quick... brown .. fox... '    
    assert split(text) == ['', '', '', 'The', 'quick...', 'brown', '..', 'fox...', '']
    assert split(text,[]) == ['', '', '', 'The', 'quick...', 'brown', '..', 'fox...', '']
    assert split(text,[' ']) == ['', '', '', 'The', 'quick...', 'brown', '..', 'fox...', '']
    assert split(text,' ') == ['', '', '', 'The', 'quick...', 'brown', '..', 'fox...', '']

    assert split(text, [' ', '.'], True) == ['The', 'quick', 'brown', 'fox']
    text = dict(a = 'Can split this', b = '..and split this too')
    assert split(text, [' ', '.'], True) == {'a': ['Can', 'split', 'this'], 'b': ['and', 'split', 'this', 'too']}

def test_lower():
    assert lower(['The Brown Fox',1]) == ['the brown fox',1]
    assert lower(dict(a = 'The Brown Fox', b = 3.0)) ==  {'a': 'the brown fox', 'b': 3.0}

def test_upper():
    assert upper(['The Brown Fox',1]) == ['THE BROWN FOX',1]
    assert upper(dict(a = 'The Brown Fox', b = 3.0)) ==  {'a': 'THE BROWN FOX', 'b': 3.0}

def test_proper():
    assert proper(['THE BROWN FOX',1]) == ['The Brown Fox',1]
    assert proper(dict(a = 'THE BROWN FOX', b = 3.0)) ==  {'a': 'The Brown Fox', 'b': 3.0}

def test_strip():
    assert strip([' whatever you say  ','  whatever you do..   ']) == ['whatever you say', 'whatever you do..']
    assert strip(dict(a = ' whatever you say  ', b = 3.0)) ==  {'a': 'whatever you say', 'b': 3.0}

def test_capitalize():
    assert capitalize('alan howard') == 'Alan howard' # use proper to get Alan Howard
    assert capitalize(['alan howard', 'donald trump']) == ['Alan howard', 'Donald trump'] # use proper?

def test_bbgcase():
    assert bbgcase('spx index') == 'SPX Index'
    assert bbgcase('spx cmon index') == 'SPX CMON Index'
    assert bbgcase('spx index 1') == 'SPX Index 1'
    assert bbgcase('spx index <go>') == 'SPX Index <GO>'
    assert bbgcase(43) == 43
    
    
def test_common_prefix():
    assert common_prefix(['abra', 'abba', 'abacus']) == 'ab'
    assert common_prefix('abra', 'abba', 'abacus') == 'ab'
    assert common_prefix() is None
    assert common_prefix([1,2,3,4], [1,2,3,5,8]) == [1,2,3]

def test_deprefix():
    assert deprefix('abra', 'abba') == ['ra', 'ba']
    assert deprefix('abra', 'abba', sep ='_') == ['abra', 'abba']
    assert deprefix('lhs_a', 'lhs_b', sep ='_') == ['a', 'b']


