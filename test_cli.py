
import unittest

from athena_cli import Athena, AthenaShell


class TestCLI(unittest.TestCase):

    def setUp(self):
        pass

    def test_use_schema(self):

        athena = Athena(profile=None, region='eu-west-1', bucket='s3://', debug=False)

        shell = AthenaShell(athena, db='sampledb')
        self.assertEqual(shell.dbname, 'sampledb')
        shell.do_use('clean')
        self.assertEqual(shell.dbname, 'clean')
