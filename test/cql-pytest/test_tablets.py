# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Some tests for the new "tablets"-based replication, replicating the old
# "vnodes". Eventually, ScyllaDB will use tablets by default and all tests
# will run using tablets, but these tests are for specific issues discovered
# while developing tablets that didn't exist for vnodes. Note that most tests
# for tablets require multiple nodes, and are in the test/topology*
# directory, so here we'll probably only ever have a handful of single-node
# tests.
#############################################################################

import pytest
from util import new_test_keyspace, new_test_table, unique_name, new_materialized_view, new_secondary_index
from cassandra.protocol import ConfigurationException, InvalidRequest

# A fixture similar to "test_keyspace", just creates a keyspace that enables
# tablets with initial_tablets=128
# The "initial_tablets" feature doesn't work if the "tablets" experimental
# feature is not turned on; In such a case, the tests using this fixture
# will be skipped.
@pytest.fixture(scope="module")
def test_keyspace_128_tablets(cql, this_dc):
    name = unique_name()
    try:
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "': 1 } AND TABLETS = { 'enabled': true, 'initial': 128 }")
    except ConfigurationException:
        pytest.skip('Scylla does not support initial_tablets, or the tablets feature is not enabled')
    yield name
    cql.execute("DROP KEYSPACE " + name)

# In the past (issue #16493), repeatedly creating and deleting a table
# would leak memory. Creating a table with 128 tablets would make this
# leak 128 times more serious and cause a failure faster. This is a
# reproducer for this problem. We basically expect this test not to
# OOM Scylla - the test doesn't "check" anything, the way it failed was
# for Scylla to run out of memory and then fail one of the CREATE TABLE
# or DROP TABLE operations in the loop.
# Note that this test doesn't even involve any data inside the table.
# Reproduces #16493.
def test_create_loop_with_tablets(cql, test_keyspace_128_tablets):
    table = test_keyspace_128_tablets + "." + unique_name()
    for i in range(100):
        cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        cql.execute("DROP TABLE " + table)


# Converting vnodes-based keyspace to tablets-based in not implemented yet
def test_alter_cannot_change_vnodes_to_tablets(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1' } AND TABLETS = { 'enabled' : false }"
    with new_test_keyspace(cql, ksdef) as keyspace:
        with pytest.raises(InvalidRequest, match="Cannot alter replication strategy vnode/tablets flavor"):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': 1}};")


# Converting vnodes-based keyspace to tablets-based in not implemented yet
def test_alter_doesnt_enable_tablets(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy'}};")

        res = cql.execute(f"DESCRIBE KEYSPACE {keyspace}").one()
        assert "NetworkTopologyStrategy" in res.create_statement

        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'")
        assert len(list(res)) == 0, "tablets replication strategy turned on"


def test_tablet_default_initialization(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'").one()
        assert res.initial_tablets == 0, "initial_tablets not configured"

        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
            table = table.split('.')[1]
            res = cql.execute("SELECT * FROM system.tablets")
            for row in res:
                if row.keyspace_name == keyspace and row.table_name == table:
                    assert row.tablet_count > 0, "zero tablets allocated"
                    break
            else:
                assert False, "tablets not allocated"


def test_tablets_can_be_explicitly_disabled(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND TABLETS = {'enabled': false};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'")
        assert len(list(res)) == 0, "tablets replication strategy turned on"


def test_alter_changes_initial_tablets(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': 2}};")
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'").one()
        assert res.initial_tablets == 2


# Test that initial number of tablets is preserved in describe
def test_describe_initial_tablets(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1' } " \
            "AND TABLETS = { 'initial' : 1 }"
    with new_test_keyspace(cql, ksdef) as keyspace:
        desc = cql.execute(f"DESCRIBE KEYSPACE {keyspace}")
        assert "and tablets = {'initial': 1}" in desc.one().create_statement.lower()


def test_tablets_are_dropped_when_dropping_table(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1'} AND TABLETS = {'initial': 1}"
    with new_test_keyspace(cql, ksdef) as keyspace:
        keyspace_name = keyspace
        table_name = ""
        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{table_name}' ALLOW FILTERING")
            assert res is not None, f"table {keyspace_name}.{table_name} not found in system.tablets after the table was created"
            assert res.one().tablet_count > 0, f"table {keyspace_name}.{table_name}: zero tablets allocated"
        res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{table_name}' ALLOW FILTERING")
        assert not res, f"table {keyspace_name}.{table_name} was found in system.tablets after the table was dropped: tablet_count={res.one().tablet_count}"


def test_tablets_are_dropped_when_dropping_view(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1'} AND TABLETS = {'initial': 1}"
    with new_test_keyspace(cql, ksdef) as keyspace:
        keyspace_name = keyspace
        table_name = ""
        view_name = ""
        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            with new_materialized_view(cql, table, '*', 'c, pk', 'c is not null and pk is not null') as mv:
                view_name = mv.split('.')[1]
                for name in [table_name, view_name]:
                    desc = "table" if name == table_name else "view"
                    res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{name}' ALLOW FILTERING")
                    assert res is not None, f"{desc} {keyspace_name}.{name} not found in system.tablets after view is created"
                    assert res.one().tablet_count > 0, f"{desc} {keyspace_name}.{name}: zero tablets allocated"
            res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{view_name}' ALLOW FILTERING")
            assert not res, f"view {keyspace_name}.{view_name} was found in system.tablets after the view was dropped: tablet_count={res.one().tablet_count}"
        res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{table_name}' ALLOW FILTERING")
        assert not res, f"table {keyspace_name}.{table_name} was found in system.tablets after the view was dropped: tablet_count={res.one().tablet_count}"


def test_tablets_are_not_dropped_when_dropping_table_with_view_fails(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1'} AND TABLETS = {'initial': 1}"
    with new_test_keyspace(cql, ksdef) as keyspace:
        keyspace_name = keyspace
        table_name = ""
        view_name = ""
        with pytest.raises(InvalidRequest):
            with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
                table_name = table.split('.')[1]
                view_name = unique_name()
                where = "c is not null and pk is not null"
                view_pk = "c, pk"
                cql.execute(f"CREATE MATERIALIZED VIEW {keyspace_name}.{view_name} AS SELECT * FROM {table} WHERE {where} PRIMARY KEY ({view_pk})")
                for name in [table_name, view_name]:
                    desc = "table" if name == table_name else "view"
                    res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{name}' ALLOW FILTERING")
                    assert res is not None, f"{desc} {keyspace_name}.{name} not found in system.tablets after view is created"
                    assert res.one().tablet_count > 0, f"{desc} {keyspace_name}.{name}: zero tablets allocated"
        for name in [table_name, view_name]:
            desc = "table" if name == table_name else "view"
            res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{name}' ALLOW FILTERING")
            assert res, f"{desc} {keyspace_name}.{name} not found in system.tablets after table drop failed"
            assert res.one().tablet_count > 0, f"{desc} {keyspace_name}.{name}: zero tablets allocated after table drop failed"


def test_tablets_are_dropped_when_dropping_index(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1'} AND TABLETS = {'initial': 1}"
    with new_test_keyspace(cql, ksdef) as keyspace:
        keyspace_name = keyspace
        table_name = ""
        index_name = ""
        system_tablets_query = ""
        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            with new_secondary_index(cql, table, "c") as si:
                index_name = si.split('.')[1] + "_index"
                for name in [table_name, index_name]:
                    desc = "table" if name == table_name else "index"
                    res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{name}' ALLOW FILTERING")
                    assert res is not None, f"{desc} {keyspace_name}.{name} not found in system.tablets after index is created"
                    assert res.one().tablet_count > 0, f"{desc} {keyspace_name}.{name}: zero tablets allocated"
            res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{index_name}' ALLOW FILTERING")
            assert not res, f"index {keyspace_name}.{index_name} was found in system.tablets after index was dropped"
        res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{table_name}' ALLOW FILTERING")
        assert not res, f"table {keyspace_name}.{table_name} was found in system.tablets after table was dropped"


def test_tablets_are_dropped_when_dropping_table_with_index(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1'} AND TABLETS = {'initial': 1}"
    with new_test_keyspace(cql, ksdef) as keyspace:
        keyspace_name = keyspace
        table_name = ""
        index_name = ""
        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
            table_name = table.split('.')[1]
            index_create_name = unique_name()
            index_name = index_create_name + "_index"
            cql.execute(f"CREATE INDEX {index_create_name} ON {table} (c)")
            for name in [table_name, index_name]:
                desc = "table" if name == table_name else "index"
                res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{name}' ALLOW FILTERING")
                assert res is not None, f"{desc} {keyspace_name}.{name} not found in system.tablets after index is created"
                assert res.one().tablet_count > 0, f"{desc} {keyspace_name}.{name}: zero tablets allocated"
        for name in [table_name, index_name]:
            desc = "table" if name == table_name else "index"
            res = cql.execute(f"SELECT * FROM system.tablets WHERE keyspace_name='{keyspace_name}' AND table_name='{name}' ALLOW FILTERING")
            assert not res, f"{desc} {keyspace_name}.{name} was found in system.tablets after table was dropped"
