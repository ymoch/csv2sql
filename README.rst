=======
CSV2SQL
=======

Convert CSV data into SQL.

.. image:: https://travis-ci.org/ymoch/csv2sql.svg?branch=master
    :target: https://travis-ci.org/ymoch/csv2sql
.. image:: https://coveralls.io/repos/github/ymoch/csv2sql/badge.svg?branch=master
    :target: https://coveralls.io/github/ymoch/csv2sql?branch=master

Features
========

- Automatic type inference.
- Customizable type deciding rules.
- Pipe usage.


Installation
============

Run ``pip install csv2sql``.


Execution
=========

For basic usage, run the command below.

.. code-block:: shell

    csv2sql all < foo.csv

For details, run the command with the `-h` option.


Details
=======

Customize Type Deciding Rules
-----------------------------

(Under construction...)

To apply your original rule,
run the command with `-p` or `--pattern-file` option.

To see the default rules on a certain query engine,
run the pattern dumping such as below.

.. code-block:: shell

    csv2sql pattern -q psql


License
=======

.. image:: https://img.shields.io/badge/License-MIT-brightgreen.svg
    :target: https://opensource.org/licenses/MIT
