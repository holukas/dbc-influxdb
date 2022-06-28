import unittest

from dbc_influxdb.main import dbcInflux


class DatabaseConnection(unittest.TestCase):
    def test_db_connection(self):
        dirconf = r'L:\Dropbox\luhk_work\20 - CODING\22 - POET\configs'
        dbc = dbcInflux(dirconf=dirconf)
        bucketlist = dbc.show_buckets()
        measurements = dbc.show_measurements_in_bucket(bucket='ch-aws_raw')
        fieldslist = dbc.show_fields_in_bucket(bucket='ch-aws_raw')
        fieldslist2 = dbc.show_fields_in_measurement(bucket='ch-aws_raw', measurement='TA')
        self.assertEqual(type(dbc), dbcInflux)
        self.assertEqual(type(bucketlist), list)
        self.assertEqual(type(measurements), list)
        self.assertEqual(type(fieldslist), list)
        self.assertEqual(type(fieldslist2), list)
        # self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
