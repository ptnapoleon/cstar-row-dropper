from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from docopt import docopt

import logging
import time

logging.basicConfig()
LOG = logging.getLogger('row-dropper')

__doc__ = """Usage: row-dropper.py [-h HOST] [-p PORT] -k KEYSPACE -t TABLE -c COLUMN

Deletes the row containing the largest value for the column specified by `-c`

Options:
    --help  Show this help message
    -h --host HOST  The IP of the C* node to connect to [default: 127.0.0.1]
    -p --port PORT  The native protocol port of the C* node to connect to [default: 9042]
    -k --keyspace KEYSPACE  The keyspace to use
    -t --table TABLE  The table to read from
    -c --column COLUMN   The name of the column to filter on
"""

def main(arguments):
    contact_ip = arguments['--host']
    port = arguments['--port']
    keyspace = arguments['--keyspace']
    table = arguments['--table']
    filter_column = arguments['--column']

    cluster = Cluster([contact_ip], port=int(port))
    session = cluster.connect(keyspace)

    session.execute("CREATE INDEX IF NOT EXISTS rowdropper ON {} ({})".format(table, filter_column))

    def index_is_built(session, keyspace):
        return len(list(session.execute(
            """SELECT * FROM system."IndexInfo"
               WHERE table_name ='{}' AND index_name='rowdropper'""".format(keyspace)))) == 1

    table_metadata = cluster.metadata.keyspaces[keyspace].tables[table]
    partition_key_columns = table_metadata.partition_key
    primary_key_columns = table_metadata.primary_key

    if len(partition_key_columns) > 1:
        LOG.error("Cannot have a compound partition key.")
        exit(1)

    primary_key_column_names = []
    for column in primary_key_columns:
        if filter_column == column.name:
            LOG.error("Cannot filter on a column in the primary key.")
            exit(1)
        primary_key_column_names += [column.name]

    primary_key_column_select = ', '.join(primary_key_column_names)
    partition_key_column = partition_key_columns[0]

    while not index_is_built(session, keyspace):
        LOG.debug("Waiting for index to finish building.")
        time.sleep(1)

    highest_filter_value = list(session.execute("SELECT max({}) FROM {}".format(filter_column, table)))[0][0]

    session.row_factory = dict_factory
    row_to_delete = list(session.execute("SELECT {} FROM {} WHERE {} = {}".format(primary_key_column_select, table, filter_column, highest_filter_value)))[0]

    delete_strings = []
    for k, v in row_to_delete.iteritems():
        delete_strings += ['{}={}'.format(k, v)]

    full_delete_string = ' and '.join(delete_strings)
    session.execute("DELETE FROM {} WHERE {}".format(table, full_delete_string))

    session.execute("DROP INDEX rowdropper") # Need this as a workaround for CASSANDRA-11331

if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
