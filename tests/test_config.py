import unittest
from unittest.mock import patch, Mock, mock_open
from parameterized import parameterized
from dgraphpandas.config import get_from_config, _get_config


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

    def test_get_config_null_config(self):
        '''
        Ensures when the config is null
        an exception is raised
        '''
        config = None
        with self.assertRaises(ValueError):
            _get_config(config)

    @patch('builtins.open', callable=mock_open)
    @patch('dgraphpandas.config.json.load')
    def test_get_config_file(self, mock_json: Mock, mock_open: Mock):
        '''
        Ensures when a file (str) is passed, open is called and
        a json is loaded
        '''
        config = 'my_config'
        expected_config = {'my_config': 'hello'}
        mock_json.return_value = expected_config

        result = _get_config(config)

        self.assertEqual(expected_config, result)

        args, kwargs = mock_open.call_args_list[0]
        self.assertEqual(args, (config, 'r'))
        self.assertEqual(kwargs, {})

    def test_get_config_passed(self):
        '''
        Ensures when an actual config is passed,
        it is returned back
        '''
        config = {'my_config': 'hello'}
        returned = _get_config(config)
        self.assertEqual(config, returned)
