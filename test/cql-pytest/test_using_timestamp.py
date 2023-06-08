# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Various tests for USING TIMESTAMP support in Scylla. Note that Cassandra
# also had tests for timestamps, which we ported in
# cassandra_tests/validation/entities/json_timestamp.py. The tests here are
# either additional ones, or focusing on more esoteric issues or small tests
# aiming to reproduce bugs discovered by bigger Cassandra tests.
#############################################################################

from util import unique_name, new_test_table, unique_key_int
from cassandra.protocol import FunctionFailure, InvalidRequest
import pytest
import time

@pytest.fixture(scope="session")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (k int PRIMARY KEY, v int)")
    yield table
    cql.execute("DROP TABLE " + table)

# In Cassandra, timestamps can be any *signed* 64-bit integer, not including
# the most negative 64-bit integer (-2^63) which for deletion times is
# reserved for marking *not deleted* cells.
# As proposed in issue #5619, Scylla forbids timestamps higher than the
# current time in microseconds plus three days. Still, any negative is
# timestamp is still allowed in Scylla. If we ever choose to expand #5619
# and also forbid negative timestamps, we will need to remove this test -
# but for now, while they are allowed, let's test that they are.
def test_negative_timestamp(cql, table1):
    p = unique_key_int()
    write = cql.prepare(f"INSERT INTO {table1} (k, v) VALUES (?, ?) USING TIMESTAMP ?")
    read = cql.prepare(f"SELECT writetime(v) FROM {table1} where k = ?")
    # Note we need to order the loop in increasing timestamp if we want
    # the read to see the latest value:
    for ts in [-2**63+1, -100, -1]:
        print(ts)
        cql.execute(write, [p, 1, ts])
        assert ts == cql.execute(read, [p]).one()[0]
    # The specific value -2**63 is not allowed as a timestamp - although it
    # is a legal signed 64-bit integer, it is reserved to mean "not deleted"
    # in the deletion time of cells.
    with pytest.raises(InvalidRequest, match='bound'):
        cql.execute(write, [p, 1, -2**63])

# As explained above, after issue #5619 Scylla can forbid timestamps higher
# than the current time in microseconds plus three days. This test will
# check that it actually does. Starting with #12527 this restriction can
# be turned on or off, so this test checks which mode we're in that this
# mode does the right thing. On Cassandra, this checking is always disabled.
def test_futuristic_timestamp(cql, table1):
    # The USING TIMESTAMP checking assumes the timestamp is in *microseconds*
    # since the UNIX epoch. If we take the number of *nanoseconds* since the
    # epoch, this will be thousands of years into the future, and if USING
    # TIMESTAMP rejects overly-futuristic timestamps, it should surely reject
    # this one.
    futuristic_ts = int(time.time()*1e9)
    p = unique_key_int()
    # In Cassandra and in Scylla with restrict_future_timestamp=false,
    # futuristic_ts can be successfully written and then read as-is. In
    # Scylla with restrict_future_timestamp=true, it can't be written.
    def restrict_future_timestamp():
        # If not running on Scylla, futuristic timestamp is not restricted
        names = [row.table_name for row in cql.execute("SELECT * FROM system_schema.tables WHERE keyspace_name = 'system'")]
        if not any('scylla' in name for name in names):
            return False
        # In Scylla, we check the configuration via CQL.
        v = list(cql.execute("SELECT value FROM system.config WHERE name = 'restrict_future_timestamp'"))
        return v[0].value == "true"
    if restrict_future_timestamp():
        print('checking with restrict_future_timestamp=true')
        with pytest.raises(InvalidRequest, match='into the future'):
            cql.execute(f'INSERT INTO {table1} (k, v) VALUES ({p}, 1) USING TIMESTAMP {futuristic_ts}')
    else:
        print('checking with restrict_future_timestamp=false')
        cql.execute(f'INSERT INTO {table1} (k, v) VALUES ({p}, 1) USING TIMESTAMP {futuristic_ts}')
        assert [(futuristic_ts,)] == cql.execute(f'SELECT writetime(v) FROM {table1} where k = {p}')

# Currently, writetime(k) is not allowed for a key column. Neither is ttl(k).
# Scylla issue #14019 and CASSANDRA-9312 consider allowing it - with the
# meaning that it should return the timestamp and ttl of a row marker.
# If this issue is ever implemented in Scylla or Cassandra, the following
# test will need to be replaced by a test for the new feature instead of
# expecting an error message.
def test_key_writetime(cql, table1):
    with pytest.raises(InvalidRequest, match='PRIMARY KEY part k'):
        cql.execute(f'SELECT writetime(k) FROM {table1}')
    with pytest.raises(InvalidRequest, match='PRIMARY KEY part k'):
        cql.execute(f'SELECT ttl(k) FROM {table1}')

# Reproducer for https://github.com/scylladb/scylladb/issues/14182
def test_rewrite_using_same_timeout_and_ttl(scylla_only, cql, table1):
    table = table1
    k = unique_key_int()
    ts1 = 1000
    ttl1 = 10
    ts2 = 1000
    ttl2 = 10
    values = [[1, 1], [1, 2], [2, 1]]
    errors = []
    for i in range(3):
        v1, v2 = values[i]

        t1 = time.time()
        cql.execute(f"INSERT INTO {table} (k, v) VALUES ({k}, {v1}) USING TIMESTAMP {ts1} AND TTL {ttl1}")

        # rewriting right away, so that both cells will carry the same expiry and ttl should return the larger value
        rewrite = f"INSERT INTO {table} (k, v) VALUES ({k}, {v2}) USING TIMESTAMP {ts2} AND TTL {ttl2}"
        cql.execute(rewrite)
        select = f"SELECT * FROM {table} WHERE k = {k}"
        res = list(cql.execute(select))
        expected = v1 if v1 >= v2 else v2
        if res[0].v != expected:
            errors.append(f"Expected (k={k}, v={expected}) after immediate rewrite in iteration {i}, but got {res[0]}")

        delay = 4
        time.sleep(delay)
        t2 = time.time()
        cql.execute(rewrite)
        res = list(cql.execute(select))
        # rewriting where the atomic_cell's expiration time should return the latter value
        expected = v2
        if res[0].v != expected:
            errors.append(f"Expected (k={k}, v={expected}) after {delay} seconds rewrite in iteration {i}, but got {res[0]}")

        expire1 = t1 + ttl1
        time.sleep(expire1 - time.time() + 2)
        res = list(cql.execute(select))
        # reading after the first write expires should return the latter value
        expected = v2
        if res[0].v != expected:
            errors.append(f"Expected (k={k}, v={expected}) after first expiration in iteration {i}, but got {res[0]}")

        expire2 = t2 + ttl2
        time.sleep(expire2 - time.time() + 2)
        # reading after both writes expired should return nothing
        res = list(cql.execute(select))
        if len(res) != 0:
            errors.append(f"Expected no results after both writes expired in iteration {i}, but got {res}")

    perrors = '\n'.join(errors)
    assert not errors, f"Found errors:\n{perrors}"
