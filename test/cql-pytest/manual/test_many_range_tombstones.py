from util import new_test_table
from nodetool import flush
import pytest

def test_populate_cache_with_many_range_tombstones(cql, test_keyspace):
    partitions = 10
    rows = 10000
    with new_test_table(cql, test_keyspace,
            "p int, c float, attribute text, another text, PRIMARY KEY (p, c)",
            extra="WITH compaction = { 'class' : 'NullCompactionStrategy' }") as table:
        content = 'x' * 100
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, attribute, another) VALUES (?, ?, ?, '{content}')")
        for p in range(partitions):
            for i in range(rows):
                cql.execute(stmt, [p, i, str(i)])
        # Flush to reconcile with following range deletions
        # Otherwise, the memtable will be compacted on flush, post
        # bcadd8229b0345c50494e33ed4b3b7e3bd21cd85
        flush(cql, table)
        # Range-delete all items except the first and last
        stmt = cql.prepare(f"DELETE FROM {table} WHERE p=? and c>=? AND c<?")
        for p in range(partitions):
            for i in range(1, rows-1):
                cql.execute(stmt, [p, i, i+1])
        flush(cql, table)
        response = list(cql.execute(f"SELECT p, c FROM {table} BYPASS CACHE"))
        print(response)
        assert len(response) == partitions * 2
