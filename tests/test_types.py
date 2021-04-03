import unittest

from dgraphpandas.types import find_rdf_types, default_rdf_type


class TypesTests(unittest.TestCase):

    def test_find_rdf_types_null_types_exception(self):
        '''
        Ensures when the types passed are null,
        then a value error is raised.
        '''
        with self.assertRaises(ValueError):
            find_rdf_types(None)

    def test_find_rdf_types_some_types_resolvable(self):
        '''
        Ensures when some of the types are resolvable
        to rdf those are resolved but the unresolvable
        are given the default rdf type.
        '''
        types = {
            'column1': 'int32',
            'column2': 'float32',
            'column3': 'something_unknown'
        }

        expected = {
            'column1': '<xs:int>',
            'column2': '<xs:float>',
            'column3': default_rdf_type
        }

        resolved_types = find_rdf_types(types)
        self.assertEqual(expected, resolved_types)

    def test_find_rdf_types_all_resolvable(self):
        '''
        Ensures when all the types are resolvable,
        they are all resolved.
        '''
        types = {
                'column1': 'object',
                'column2': 'O',
                'column3': 'int32',
                'column4': 'int64',
                'column5': 'int32',
                'column6': 'int64',
                'column7': 'float32',
                'column8': 'float64',
                'column9': 'float32',
                'column10': 'float64',
                'column11': 'datetime64',
                'column12': '<M8[ns]',
                'column13': 'bool'
        }

        expected = {
                'column1': '<xs:string>',
                'column2': '<xs:string>',
                'column3': '<xs:int>',
                'column4': '<xs:int>',
                'column5': '<xs:int>',
                'column6': '<xs:int>',
                'column7': '<xs:float>',
                'column8': '<xs:float>',
                'column9': '<xs:float>',
                'column10': '<xs:float>',
                'column11': '<xs:dateTime>',
                'column12': '<xs:dateTime>',
                'column13': '<xs:bool>'
        }

        resolved_types = find_rdf_types(types)
        self.assertEqual(expected, resolved_types)

    def test_find_rdf_types_none_resolvable(self):
        '''
        Ensures when none of the columns can be resolved,
        they are all resolved to default rdf type
        '''
        types = {
                'column1': 'unknown',
                'column2': 'unknown2',
                'column3': 'unknown3',
                'column4': 'unknown4'
        }

        resolved_types = find_rdf_types(types)
        self.assertEqual(list(resolved_types.values()), [default_rdf_type]*4)
