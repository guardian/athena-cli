Athena CLI
==========

[![Build Status](https://travis-ci.org/guardian/athena-cli.svg?branch=master)](https://travis-ci.org/guardian/athena-cli)

Presto-like CLI tool for [AWS Athena](https://aws.amazon.com/athena/). The alternative is using the
AWS CLI [Athena sub-commands](http://docs.aws.amazon.com/cli/latest/reference/athena/).

[![asciicast](https://asciinema.org/a/132545.png)](https://asciinema.org/a/132545)

Installation
------------

To install using `pip` run:

    $ pip install athena-cli

Or, clone the GitHub repo and run:

    $ python setup.py install

Configuration
-------------

Only required configuration is AWS credentials.

Usage
-----

```
$ athena --help
usage: athena [--debug] [--execute <statement>] [--output-format <format>] [--schema <schema>]
              [--profile <profile>] [--region <region>] [--s3-bucket <bucket>]
              [--server-side-encryption] [--version]

Athena interactive console

optional arguments:
  -h, --help            show this help message and exit
  --debug               enable debug mode
  --execute STATEMENT   execute statement in batch mode
  --output-format FORMAT
                        output format for batch mode [ALIGNED, VERTICAL, CSV,
                        TSV, CSV_HEADER, TSV_HEADER, NULL]
  --schema SCHEMA, --database SCHEMA, --db SCHEMA
                        default schema
  --profile PROFILE     AWS profile
  --region REGION       AWS region
  --s3-bucket BUCKET, --bucket BUCKET
                        AWS S3 bucket for query results
  --server-side-encryption, --encryption
                        Use server-side-encryption for query results
  --version             show version info and exit
```

```
athena> help

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

athena> use clean;
athena:clean> show tables;
tab_name86a314-ecda-45e9-aec1-b0a3c4d7f1e2, SUCCEEDED
-------------
elb_logs_raw_native
elb_logs_raw_native_part
(2 rows)

Query b586a314-ecda-45e9-aec1-b0a3c4d7f1e2, SUCCEEDED
https://eu-west-1.console.aws.amazon.com/athena/home?force&region=eu-west-1#query/history/b586a314-ecda-45e9-aec1-b0a3c4d7f1e2
Time: 0:00:01, CPU Time: 1150ms total, Data Scanned: 0.00B, Cost: $0.00

```

Troubleshooting
---------------

Use the `--debug` option when launching the `athena` CLI to get AWS debug output:

```shell
$ athena --debug
2017-07-21 10:10:45,477 botocore.credentials [DEBUG] Looking for credentials via: env
2017-07-21 10:10:45,478 botocore.credentials [DEBUG] Looking for credentials via: assume-role
2017-07-21 10:10:45,478 botocore.credentials [DEBUG] Looking for credentials via: shared-credentials-file
2017-07-21 10:10:45,479 botocore.credentials [INFO] Found credentials in shared credentials file: ~/.aws/credentials
...
```

Turn on debug at the `athena>` prompt by typing:

```
athena> set debug true
debug - was: False
now: True
```

Command history is written to `~/.athena_history`.

References
----------

  * AWS Athena: https://aws.amazon.com/athena/
  * AWS SDK for Python: https://boto3.readthedocs.io/en/latest/reference/services/athena.html
  * PrestoDB: https://prestodb.io/docs/current/

License
-------

    Athena CLI
    Copyright 2017 Guardian News & Media

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
