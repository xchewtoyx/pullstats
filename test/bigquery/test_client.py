import os
import shutil
import tempfile
import unittest

from apiclient.http import HttpMockSequence

from pullstats.bigquery import client

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

def datafile(name):
    return os.path.join(TEST_DATA_DIR, name)

class TestValidator(client.BaseValidator):
    def integer(self, value):
        return int(value)

    def cv_calls(self, value):
        return int(value)

class ValidatorTest(unittest.TestCase):
    def test_defaul_function(self):
        validator = client.BaseValidator()
        self.assertEqual('foo', validator._default_validator('foo'))

    def test_class_default(self):
        validator = client.BaseValidator()
        test_values = ['foo', 1, '1', 1.0, '1.0' ]

        for value in test_values:
            self.assertEqual(validator('bar', value), value)

    def test_no_default(self):
        validator = client.BaseValidator(register_default=False)
        with self.assertRaises(AttributeError):
            validator('foo', 'bar')

    def test_custom_validator(self):
        validator = TestValidator(register_default=False)

        self.assertNotEqual('1', validator('integer', '1'))
        self.assertEqual(1, validator('integer', '1'))
        with self.assertRaises(ValueError):
            validator('integer', 'foo')


class ClientTest(unittest.TestCase):
    def test_client(self):
        bq_client = client.BigQueryClient('12345', TEST_DATA_DIR)
        bq_client.http = HttpMockSequence([
            ({'status': 200}, open(datafile('bigquery_v2.json')).read()),
            ({'status': 200}, open(datafile('response.json')).read()),
        ])
        results = bq_client.query('SELECT something FROM somewhere')

        self.assertEqual(5, len(results))
        self.assertEqual('8', results[0]['cv_calls'])

    def test_client_validated(self):
        validator = TestValidator(register_default=True)
        bq_client = client.BigQueryClient('12345', TEST_DATA_DIR)
        bq_client.http = HttpMockSequence([
            ({'status': 200}, open(datafile('bigquery_v2.json')).read()),
            ({'status': 200}, open(datafile('response.json')).read()),
        ])
        results = bq_client.query('SELECT something FROM somewhere',
                                  validator=validator)

        self.assertEqual(5, len(results))
        self.assertEqual(8, results[0]['cv_calls'])
