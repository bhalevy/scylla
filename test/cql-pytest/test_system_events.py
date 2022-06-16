# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
import util
import nodetool

def get_system_events(cql):
    events = dict()
    rows = list(cql.execute("SELECT category, started_at, type, params FROM system.events"))
    assert(len(rows) > 0)
    for row in rows:
        if not row.category in events:
            events[row.category] = dict()
        if not row.type in events[row.category]:
            events[row.category][row.type] = dict()
        events[row.category][row.type][row.started_at] = row.params
    return events

# Check reading the system.events table, which should list major system events
# like start/stop.
def test_system_events(scylla_only, cql, this_dc):
    events = get_system_events(cql)
    assert len(events['system']['start']) > 0

    test_keyspace = ""
    with util.new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}") as test_keyspace:
        events = get_system_events(cql)
        keyspace_found = False
        search_str = f"keyspace_name={test_keyspace}"
        for e in events['database']['create_keyspace'].values():
            keyspace_found = search_str in e
        assert keyspace_found, f"'{test_keyspace}' not found in: {events['database']['create_keyspace']}"

        test_table = ""
        with util.new_test_table(cql, test_keyspace, "k int PRIMARY KEY") as test_table:
            events = get_system_events(cql)
            table_found = False
            keyspace_search_str = f"keyspace_name={test_keyspace}"
            table_search_str = f"table_name={test_table.split('.')[1]}"
            for e in events['database']['create_table'].values():
                table_found = keyspace_search_str in e and table_search_str in e
            assert table_found, f"'{test_table}' not found in: {events['database']['create_table']}"

        events = get_system_events(cql)
        table_found = False
        keyspace_search_str = f"keyspace_name={test_keyspace}"
        table_search_str = f"table_name={test_table.split('.')[1]}"
        for e in events['database']['drop_table'].values():
            table_found = keyspace_search_str in e and table_search_str in e
        assert table_found, f"'{test_table}' not found in: {events['database']['drop_table']}"

    events = get_system_events(cql)
    keyspace_found = False
    search_str = f"keyspace_name={test_keyspace}"
    for e in events['database']['drop_keyspace'].values():
        keyspace_found = search_str in e
    assert keyspace_found, f"'{test_keyspace}' not found in: {events['database']['drop_keyspace']}"

def test_major_compaction(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "k int PRIMARY KEY") as test_table:
        cql.execute(f"INSERT INTO {test_table} (k) VALUES (0)")
        nodetool.flush(cql, test_table)
        nodetool.compact(cql, test_table)
        events = get_system_events(cql)
        table_found = False
        keyspace_search_str = f"keyspace_name={test_keyspace}"
        table_search_str = f"table_name={test_table.split('.')[1]}"
        for e in events['compaction']['major'].values():
            table_found = keyspace_search_str in e and table_search_str in e
        assert table_found, f"'{test_table}' not found in: {events['compaction']['major']}"

def test_repair(scylla_only, cql, test_keyspace):
    with util.new_test_table(cql, test_keyspace, "k int PRIMARY KEY") as test_table:
        cql.execute(f"INSERT INTO {test_table} (k) VALUES (0)")
        nodetool.flush(cql, test_table)
        nodetool.repair(cql, test_keyspace)
        events = get_system_events(cql)
        table_found = False
        keyspace_search_str = f"keyspace_name={test_keyspace}"
        for e in events['repair']['async'].values():
            table_found = keyspace_search_str
        assert table_found, f"'{test_table}' not found in: {events['repair']['async']}"
