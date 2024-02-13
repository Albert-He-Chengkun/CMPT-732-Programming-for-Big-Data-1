from cassandra.cluster import Cluster
from datetime import datetime
import os, sys, re, uuid, gzip
from cassandra.query import BatchStatement, SimpleStatement


def main(input_dir, keyspace, table_name):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect()
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 2}}")
    session.execute(f"USE {keyspace}")
    session.execute("CREATE TABLE IF NOT EXISTS nasalogs ( id UUID, host TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (host, id) )")

    batch = BatchStatement()
    batch_count = 0
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
                split_line = line_re.split(line)
                if len(split_line) == 6:
                    batch.add(SimpleStatement("INSERT INTO nasalogs (id, host, datetime, path, bytes) VALUES (%s, %s, %s, %s, %s)"), (uuid.uuid4(), split_line[1], datetime.strptime(split_line[2],'%d/%b/%Y:%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'), split_line[3], int(split_line[4])))
                    batch_count += 1
                    if batch_count == 100:
                            session.execute(batch)
                            batch.clear()
                            batch_count = 0

    session.execute(batch)
    batch.clear()
    batch_count = 0
                    
    # session.execute("SELECT host, sum(bytes) FROM nasalogs GROUP BY host")
    session.shutdown()



if __name__=="__main__":
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)
