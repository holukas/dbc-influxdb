"""Integration test that requires a live InfluxDB and local config files.

Skipped automatically when the configuration directory is not present (e.g. in
CI), so it does not break test runs on machines without database access.
"""
import unittest
from pathlib import Path

from dbc_influxdb.main import dbcInflux

DIRCONF = r'L:\Dropbox\luhk_work\20 - CODING\22 - POET\configs'


@unittest.skipUnless(Path(DIRCONF).exists(),
                     f"Config directory not found ({DIRCONF}); skipping live DB test.")
class DatabaseConnection(unittest.TestCase):
    def test_db_connection(self):
        dbc = dbcInflux(dirconf=DIRCONF)
        bucketlist = dbc.show_buckets()
        measurements = dbc.show_measurements_in_bucket(bucket='ch-aws_raw')
        fieldslist = dbc.show_fields_in_bucket(bucket='ch-aws_raw')
        fieldslist2 = dbc.show_fields_in_measurement(bucket='ch-aws_raw', measurement='TA')
        self.assertEqual(type(dbc), dbcInflux)
        self.assertEqual(type(bucketlist), list)
        self.assertEqual(type(measurements), list)
        self.assertEqual(type(fieldslist), list)
        self.assertEqual(type(fieldslist2), list)


if __name__ == '__main__':
    unittest.main()
