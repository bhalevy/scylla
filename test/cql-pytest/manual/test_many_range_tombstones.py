from util import new_test_table
from nodetool import flush
import pytest

def test_populate_cache_with_many_range_tombstones(cql, test_keyspace):
    count = 100000
    with new_test_table(cql, test_keyspace,
            "p int, c int, attribute text, another text, PRIMARY KEY (p, c)",
            extra="WITH compaction = { 'class' : 'NullCompactionStrategy' }") as table:
        content = 'x' * 100
        stmt = cql.prepare(f"INSERT INTO {table} (p, c, attribute, another) VALUES (?, ?, ?, '{content}')")
        for i in range(count):
            cql.execute(stmt, [1, i, str(i)])
        # Flush to reconcile with following range deletions
        # Otherwise, the memtable will be compacted on flush, post
        # bcadd8229b0345c50494e33ed4b3b7e3bd21cd85
        flush(cql, table)
        # Range-delete all items except the first and last
        stmt = cql.prepare(f"DELETE FROM {table} WHERE p=? and c>=? AND c<{count-1}")
        for i in range(1, count-2):
            cql.execute(stmt, [1, i])
        flush(cql, table)
        response = list(cql.execute(f"SELECT p, c FROM {table} BYPASS CACHE"))
        print(response)
        assert len(response) == 2
