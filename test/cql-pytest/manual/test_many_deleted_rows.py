from util import new_test_table
from cassandra_tests.porting import assert_row_count
import pytest
import time

def test_scan_partition_with_many_deleted_rows(cql, test_keyspace):
    count = 400000
    with new_test_table(cql, test_keyspace,
            "p int, c int, attribute text, another text, PRIMARY KEY (p, c)",
            extra="WITH compaction = { 'class' : 'NullCompactionStrategy' }") as table:
        content = 'x' * 100
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, attribute, another) VALUES (?, ?, ?, '{content}') USING TTL 1")
        for i in range(count-1):
            cql.execute(stmt, [1, i, str(i)])
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, attribute, another) VALUES (?, ?, ?, '{content}')")
        for i in [count-1]:
            cql.execute(stmt, [1, i, str(i)])
        time.sleep(1)
        # Delete all items except the first and last
        #stmt = cql.prepare(f"DELETE FROM {table} WHERE p=? and c=?")
        #for i in range(1, count-1):
        #    cql.execute(stmt, [1, i])
        #flush(cql, table)
        res = cql.execute(f"SELECT c FROM {table} WHERE p=1 BYPASS CACHE")
        assert_row_count(res, 1)
