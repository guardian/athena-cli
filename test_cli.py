
import unittest

from athena_cli import AthenaShell


class TestCLI(unittest.TestCase):

    def setUp(self):
        pass

    def test_use_schema(self):

        shell = AthenaShell(profile=None, region='eu-west-1', bucket='s3://', db='sampledb', debug=False)
        self.assertEqual(shell.dbname, 'sampledb')
        shell.do_use('clean')
        self.assertEqual(shell.dbname, 'clean')
