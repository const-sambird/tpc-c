# TPC-C for PostgreSQL

This is an implementation of the TPC-C database benchmark for Postgres. It can create the tables and generate queries.

The full benchmark specification is available from the [TPC website](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf).

## Installation

This program assumes access to an installation of PostgreSQL with connections appropriately configured and Python 3.13. This benchmark was developed for PostgreSQL 17.

First, create a venv and install the dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Loading the data

[`load.py`](./load.py) is the data loader. It takes one optional argument: if `-c` or `--create-tables` is present, the tables and foreign key constraints will be created (as defined in [`table_def.sql`](./table_def.sql) and [`fk_def.sql`](./fk_def.sql)).

There are two positional arguments. The first is the number of warehouses to create, which is the unit of scale that TPC-C uses. The second is the Postgres connection string to connect to the database.

> ![WARNING]
> The primary keys are _not_ created by default (as I am using this utility to evaluate an index recommendation algorithm). If this is not desired behaviour, uncomment the primary keys from [`table_def.sql`](./table_def.sql) before creating the tables.

```bash
# create the tables and load 1 warehouse
python load.py -c 1 dbname=tpccdb

# load 10 warehouses, assuming the tables are already created
python load.py 10 dbname=tpccdb
```

## Running the benchmark

[`run.py`](./run.py) creates the queries and runs the benchmark. Like the data loader, it has two positional arguments. The first is the number of warehouses present in the database (which must be equal to or less than the number actually loaded). The second is the database connection string. Finally, there is one optional argument, `-n` or `--times-to-run`, to run each transaction several times.

The benchmark will write the queries it is executing to the `workload/` folder. They are grouped by the transaction as well as the query template they are executed from.

```bash
# run the benchmark 100 times on a 1 warehouse database
python run.py -n 100 1 dbname=tpccdb

# run the benchmark once on a 10 warehouse database
python run.py 10 dbname=tpccdb
```

## Disclaimer

This program is a good-faith attempt to implement the TPC-C benchmark, but absolutely no guarantees are provided that it conforms to the specification given by the TPC, whether in data format, queries, or any particular requirement of the benchmark.
