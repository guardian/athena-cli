Athena CLI
==========

Presto-like CLI tool for AWS Athena.

Installation
------------

Using `pip` install then run:

    $ pip install athena-cil

Or, clone the GitHub repo and run:

    $ python setup.py install

Configuration
-------------

Only required configuration is AWS credentials.

Usage
-----

```shell
$ athena
```

```sql
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

Turn on debug at the `athena>` prompt by typing:

```
athena> set debug true
debug - was: False
now: True
```

Command history is written to `~/.athena_history`.

References
----------

  * PrestoDB: https://prestodb.io/docs/current/
  * AWS Athena: https://aws.amazon.com/athena/

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
