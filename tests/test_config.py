import unittest
from parameterized import parameterized
from dgraphpandas.config import get_from_config


class ConfigTests(unittest.TestCase):

    @parameterized.expand([
        (None, {'hello': 'world'}, 'default', {'firstkey': 'secondkey'}),
        ('key', None, 'default', {'firstkey': 'secondkey'})
    ])
    def test_get_from_config_nullkey_exception(
            self,
            key,
            config,
            default,
            kwargs):
        '''
        Ensures when the passed key is none, then an exception is raised.
        '''
        with self.assertRaises(ValueError):
            get_from_config(key, config, default, **(kwargs))

    def test_get_from_config_override_exists(self):
        '''
        Ensures when an override exists in kwargs, then it is used
        even if a value exists in config
        '''
        key = 'subject_fields'
        config = {'subject_fields': 'id'}
        value = get_from_config(key, config, subject_fields='override')

        self.assertEqual(value, 'override')

    def test_get_from_config_no_override_exists(self):
        '''
        Ensures when no override exists in kwargs, then the config value is used
        '''
        key = 'subject_fields'
        config = {'subject_fields': 'id'}
        value = get_from_config(key, config, something_else='override')

        self.assertEqual(value, 'id')

    def test_get_from_config_no_override_and_no_config_default(self):
        '''
        Ensures when no override exists in kwargs, then the config value is used
        '''
        key = 'subject_fields'
        config = {'some_other_field': 'value'}
        value = get_from_config(key, config, something_else='override', default='default_value')

        self.assertEqual(value, 'default_value')
