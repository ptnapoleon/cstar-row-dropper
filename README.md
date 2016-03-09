# cstar-row-dropper

Warning: cstar-row-dropper is an entirely fictional tool, used as a proof of concept.

Row-dropper is a tool for managing the data in your Apache Cassandra cluster. Given a keyspace, table, and column name,
row-dropper will find the row with the largest value for the given column, and delete it.

Restrictions:
- Row-dropper is only supported against C* 3.0 and higher.
- The column to filter on cannot be part of the primary key.
- The table must not have a compound partition key, but may have a clustering key.


## CLI Interface

```
Usage: row-dropper.py [-h HOST] [-p PORT] -k KEYSPACE -t TABLE -c COLUMN

Deletes the row containing the largest value for the column specified by `-c`

Options:
    --help  Show this help message
    -h --host HOST  The IP of the C* node to connect to [default: 127.0.0.1]
    -p --port PORT  The native protocol port of the C* node to connect to [default: 9042]
    -k --keyspace KEYSPACE  The keyspace to use
    -t --table TABLE  The table to read from
    -c --column COLUMN   The name of the column to filter on
```