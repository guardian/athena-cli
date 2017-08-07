#!/usr/local/bin/python

import argparse
import atexit
import csv
import json
import os
import readline
import subprocess
import sys
import time
import uuid

import boto3
import cmd2 as cmd

from botocore.exceptions import ClientError, ParamValidationError
from tabulate_presto import tabulate

LESS = "less -FXRSn"
HISTORY_FILE_SIZE = 500

__version__ = '0.0.13'


class AthenaBatch(object):

    def __init__(self, profile, region, bucket, db=None, format='CSV', debug=False):

        self.athena = Athena(profile, region, bucket, debug)

        self.region = region
        self.bucket = bucket
        self.dbname = db
        self.format = format
        self.debug = debug

    def execute(self, statement):
        self.athena.execution_id = self.athena.start_query_execution(self.dbname, statement)
        if not self.athena.execution_id:
            return

        while True:
            stats = self.athena.get_query_execution()
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.2)  # 200ms

        if status == 'SUCCEEDED':
            results = self.athena.get_query_results()
            headers = [h['Name'] for h in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

            if self.format in ['CSV', 'CSV_HEADER']:
                csv_writer = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
                if self.format == 'CSV_HEADER':
                    csv_writer.writerow(headers)
                csv_writer.writerows([x for x in self.athena.yield_rows(results, headers)])
            elif self.format == 'TSV':
                print(tabulate([x for x in self.athena.yield_rows(results, headers)], tablefmt='tsv'))
            elif self.format == 'TSV_HEADER':
                print(tabulate([x for x in self.athena.yield_rows(results, headers)], headers=headers, tablefmt='tsv'))
            elif self.format == 'VERTICAL':
                for x, row in enumerate(self.athena.yield_rows(results, headers)):
                    print('--[RECORD {}]--'.format(x+1))
                    print(tabulate(zip(*[headers,row]), tablefmt='presto'))
            else:  # ALIGNED
                print(tabulate([x for x in self.athena.yield_rows(results, headers)], headers=headers, tablefmt='presto'))

        if status == 'FAILED':
            print(stats['QueryExecution']['Status']['StateChangeReason'])


del cmd.Cmd.do_show


class AthenaShell(cmd.Cmd):

    multilineCommands = ['WITH', 'SELECT', 'ALTER', 'CREATE', 'DESCRIBE', 'DROP', 'MSCK', 'SHOW', 'USE', 'VALUES']
    allow_cli_args = False

    def __init__(self, profile, region, bucket, db=None, debug=False):
        cmd.Cmd.__init__(self)

        self.athena = Athena(profile, region, bucket, debug)

        self.region = region
        self.bucket = bucket
        self.dbname = db
        self.debug = debug

        self.row_count = 0

        self.set_prompt()
        self.pager = os.environ.get('ATHENA_CLI_PAGER', LESS).split(' ')

        self.hist_file = os.path.join(os.path.expanduser("~"), ".athena_history")
        self.init_history()

    def set_prompt(self):
        self.prompt = 'athena:%s> ' % self.dbname if self.dbname else 'athena> '

    def cmdloop_with_cancel(self, intro=None):
        try:
            self.cmdloop(intro)
        except KeyboardInterrupt:
            if self.athena.execution_id:
                self.athena.stop_query_execution()
                print('\n\n%s' % self.athena.console_link(self.region))
                print('\nQuery aborted by user')
            else:
                print('\r')
            self.cmdloop_with_cancel(intro)

    def preloop(self):
        if os.path.exists(self.hist_file):
            readline.read_history_file(self.hist_file)

    def postloop(self):
        self.save_history()

    def init_history(self):
        try:
            readline.read_history_file(self.hist_file)
            readline.set_history_length(HISTORY_FILE_SIZE)
            readline.write_history_file(self.hist_file)
        except IOError:
            readline.write_history_file(self.hist_file)

        atexit.register(self.save_history)

    def save_history(self):
        try:
            readline.write_history_file(self.hist_file)
        except IOError:
            pass

    def do_help(self, args):
        help = """
Supported commands:
QUIT
SELECT
ALTER DATABASE <schema>
ALTER TABLE <table>
CREATE DATABASE <schema>
CREATE TABLE <table>
DESCRIBE <table>
DROP DATABASE <schema>
DROP TABLE <table>
MSCK REPAIR TABLE <table>
SHOW COLUMNS FROM <table>
SHOW CREATE TABLE <table>
SHOW DATABASES [LIKE <pattern>]
SHOW PARTITIONS <table>
SHOW TABLES [IN <schema>] [<pattern>]
SHOW TBLPROPERTIES <table>
USE [<catalog>.]<schema>
VALUES row [, ...]

See http://docs.aws.amazon.com/athena/latest/ug/language-reference.html
"""
        print(help)

    def do_quit(self, args):
        print()
        return -1

    def do_EOF(self, args):
        return self.do_quit(args)

    def do_use(self, schema):
        self.dbname = schema.rstrip(';')
        self.set_prompt()

    def default(self, statement):
        self.athena.execution_id = self.athena.start_query_execution(self.dbname, statement)
        if not self.athena.execution_id:
            return

        while True:
            stats = self.athena.get_query_execution()
            status = stats['QueryExecution']['Status']['State']
            status_line = 'Query {0}, {1:9}'.format(self.athena.execution_id, status)
            sys.stdout.write('\r' + status_line)
            sys.stdout.flush()
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.2)  # 200ms

        sys.stdout.write('\r' + ' ' * len(status_line) + '\r')  # delete query status line
        sys.stdout.flush()

        if status == 'SUCCEEDED':
            results = self.athena.get_query_results()
            headers = [h['Name'] for h in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            row_count = len(results['ResultSet']['Rows'])

            if results['ResultSet']['Rows'][0]['Data'][0].get('VarCharValue', None) == headers[0]:
                row_count -= 1  # don't count header

            process = subprocess.Popen(self.pager, stdin=subprocess.PIPE)
            process.stdin.write(tabulate([x for x in self.athena.yield_rows(results, headers)], headers=headers, tablefmt='presto').encode('utf-8'))
            process.communicate()
            print('(%s rows)\n' % row_count)

        print('Query {0}, {1}'.format(self.athena.execution_id, status))
        if status == 'FAILED':
            print(stats['QueryExecution']['Status']['StateChangeReason'])
        print(self.athena.console_link(self.region))

        submission_date = stats['QueryExecution']['Status']['SubmissionDateTime']
        completion_date = stats['QueryExecution']['Status']['CompletionDateTime']
        execution_time = stats['QueryExecution']['Statistics']['EngineExecutionTimeInMillis']
        data_scanned = stats['QueryExecution']['Statistics']['DataScannedInBytes']
        query_cost = data_scanned / 1000000000000.0 * 5.0

        print('Time: {}, CPU Time: {}ms total, Data Scanned: {}, Cost: ${:,.2f}\n'.format(
            str(completion_date - submission_date).split('.')[0],
            execution_time,
            human_readable(data_scanned),
            query_cost)
        )


class Athena(object):

    def __init__(self, profile, region, bucket, debug=False):

        session = boto3.Session(profile_name=profile, region_name=region)
        self.athena = session.client('athena')

        self.bucket = bucket
        self.execution_id = None
        self.debug = debug

    def start_query_execution(self, db, query):
        try:
            if not db:
                raise ValueError('Schema must be specified when session schema is not set')

            return self.athena.start_query_execution(
                QueryString=query,
                ClientRequestToken=str(uuid.uuid4()),
                QueryExecutionContext={
                    'Database': db
                },
                ResultConfiguration={
                    'OutputLocation': self.bucket
                }
            )['QueryExecutionId']
        except (ClientError, ParamValidationError, ValueError) as e:
            print(e)
            return

    def get_query_execution(self):
        try:
            return self.athena.get_query_execution(
                QueryExecutionId=self.execution_id
            )
        except ClientError as e:
            print(e)

    def get_query_results(self):
        try:
            results = self.athena.get_query_results(
                QueryExecutionId=self.execution_id
            )
        except ClientError as e:
            sys.exit(e)

        if self.debug:
            print(json.dumps(results, indent=2))

        return results

    def stop_query_execution(self):
        try:
            return self.athena.stop_query_execution(
                QueryExecutionId=self.execution_id
            )
        except ClientError as e:
            sys.exit(e)

    @staticmethod
    def yield_rows(results, headers):
        for row in results['ResultSet']['Rows']:
            # https://forums.aws.amazon.com/thread.jspa?threadID=256505
            if row['Data'][0].get('VarCharValue', None) == headers[0]:
                continue  # skip header
            yield [d.get('VarCharValue', 'NULL') for d in row['Data']]

    def console_link(self, region):
        return 'https://{0}.console.aws.amazon.com/athena/home?force&region={0}#query/history/{1}'.format(region, self.execution_id)


def human_readable(size, precision=2):
    suffixes=['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1 #increment the index of the suffix
        size = size/1024.0 #apply the division
    return "%.*f%s"%(precision,size,suffixes[suffixIndex])


def main():

    parser = argparse.ArgumentParser(
        prog='athena',
        usage='athena [--debug] [--execute <statement>] [--output-format <format>] [--schema <schema>]'
              ' [--profile <profile>] [--region <region>] [--s3-bucket <bucket>] [--version]',
        description='Athena interactive console'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='enable debug mode'
    )
    parser.add_argument(
        '--execute',
        metavar='STATEMENT',
        help='execute statement in batch mode'
    )
    parser.add_argument(
        '--output-format',
        dest='format',
        help='output format for batch mode [ALIGNED, VERTICAL, CSV, TSV, CSV_HEADER, TSV_HEADER, NULL]'
    )
    parser.add_argument(
        '--schema',
        '--database',
        '--db',
        help='default schema'
    )
    parser.add_argument(
        '--profile',
        help='AWS profile'
    )
    parser.add_argument(
        '--region',
        help='AWS region'
    )
    parser.add_argument(
        '--s3-bucket',
        '--bucket',
        dest='bucket',
        help='AWS S3 bucket for query results'
    )
    parser.add_argument(
        '--version',
        action='store_true',
        help='show version info and exit'
    )
    args = parser.parse_args()

    if args.debug:
        boto3.set_stream_logger(name='botocore')

    if args.version:
        print('Athena CLI %s' % __version__)
        sys.exit()

    # get profile
    profile = args.profile or os.environ.get('AWS_DEFAULT_PROFILE', None)

    # get region
    try:
        region_from_profile = subprocess.check_output('aws configure get region --profile {}'
                                                      .format(profile or 'default'), shell=True).decode('utf-8').rstrip()
    except Exception:
        region_from_profile = None
    region = args.region or os.environ.get('AWS_DEFAULT_REGION', None) or region_from_profile

    if not region:
        sys.exit('You must specify a region.')

    # get account id
    try:
        account_id=subprocess.check_output('aws sts get-caller-identity --output text --query \'Account\' --profile {}'
                                           .format(profile or 'default'), shell=True).decode('utf-8').rstrip()
    except Exception as e:
        sys.exit(str(e))

    # get S3 bucket
    bucket = args.bucket or 's3://{}-query-results-{}-{}'.format(profile or 'aws-athena', account_id, region)

    if args.execute:
        batch = AthenaBatch(profile, region, bucket, db=args.schema, format=args.format, debug=args.debug)
        batch.execute(statement=args.execute)
    else:
        shell = AthenaShell(profile, region, bucket, db=args.schema, debug=args.debug)
        shell.cmdloop_with_cancel()

if __name__ == '__main__':
    main()
