#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Reproducer for SCYLLADB-6092: deleted rows resurrected by read-repair racing with an
# incremental tablet repair, in tombstone_gc=repair mode.
#
# Place in test/cluster/ and run with:
#   pytest --mode=dev test/cluster/test_scylladb_6092_repro.py

import asyncio
import json
import logging
import time

import pytest
from cassandra.query import ConsistencyLevel, SimpleStatement

from test.cluster.test_incremental_repair import parse_compaction_lineage, get_repaired_at_from_sst, inject_error_on, inject_error_off
from test.cluster.util import create_new_test_keyspace, reconnect_driver
from test.pylib.repair import load_tablet_sstables_repaired_at, load_tablet_repair_time
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)


async def insert_range(cql, ks, start, end, host=None):
    stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, ck, v) VALUES (?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.QUORUM
    await asyncio.gather(*[cql.run_async(stmt, [k, 0, k], host=host) for k in range(start, end)])


async def flush_all(manager, servers, ks):
    await asyncio.gather(*[manager.api.flush_keyspace(s.ip_addr, ks) for s in servers])


async def dump_r(manager, cql, ks, servers, hosts, scylla_path, label, pk):
    """Log where the row lives on every node: memtable/cache/sstable, and repaired_at of the sstables."""
    live = {}
    for s, h in zip(servers, hosts):
        rows = await cql.run_async(
            f"SELECT mutation_source, mutation_fragment_kind, metadata FROM MUTATION_FRAGMENTS({ks}.test) "
            f"WHERE pk = {pk}", host=h)
        ssts = sorted({r.mutation_source.split(':', 1)[1] for r in rows if r.mutation_source.startswith('sstable:')})
        repaired_at = await get_repaired_at_from_sst(ssts, scylla_path) if ssts else {}
        # Fragments are listed per source: merge the row's tombstone and live data across sources.
        max_tomb = -1
        max_live = -1
        for r in rows:
            if r.mutation_fragment_kind != 'clustering row' or not r.metadata:
                continue
            md = json.loads(r.metadata)
            max_tomb = max(max_tomb, md.get('tombstone', {}).get('timestamp', -1))
            max_live = max(max_live, md.get('marker', {}).get('timestamp', -1),
                           *[c.get('timestamp', -1) for c in md.get('columns', {}).values() if c.get('is_live')])
        has_row = max_live > max_tomb
        live[s.server_id] = has_row
        logger.info(f"[{label}] server {s.server_id} ({s.ip_addr}): live_row={has_row} max_live_ts={max_live} max_tombstone_ts={max_tomb}")
        for r in rows:
            src = r.mutation_source
            if src.startswith('sstable:'):
                src = f"{src.rsplit('/', 1)[-1]} repaired_at={repaired_at.get(src.split(':', 1)[1])}"
            logger.info(f"[{label}]   {src} {r.mutation_fragment_kind} {(r.metadata or '')[:200]}")
    return live


@pytest.mark.parametrize("read_during_repair", [True, False], ids=["read_during_repair", "no_read"])
@pytest.mark.asyncio
async def test_read_repair_during_incremental_repair_resurrects(manager: ScyllaClusterManager, read_during_repair):
    """
    SCYLLADB-6092: read-repair plants an old-timestamp copy of a deleted row in the memtable
    of a replica that holds the tombstone, after that replica took its incremental repair snapshot. When the
    repair commits, the tombstone is repaired and GC-eligible, and the repaired-view compaction purges it
    without looking at the memtable or the unrepaired sstables, where the copy of the row lives.

    1. Y misses the delete of R (it is down, hints are disabled). The holders compact R away under T.
    2. An incremental repair is paused right after the replicas took their sstable snapshots.
    3. A CL=ALL read of R's partition: Y returns R, the holders return T. Read-repair writes R's cells to the
       holders' memtables (with R's original timestamp) and T to Y's.
    4. The repair resumes and commits: T becomes repaired and GC-eligible on the holders.
    5. Major compaction on the holders purges T in the repaired view, R survives in the unrepaired view.
    6. R is readable again.

    The no_read variant is the control: without the read during the repair, nothing is resurrected.
    """
    pk = 2_000_000
    cfg = {'enable_tablets': True, 'hinted_handoff_enabled': False}
    cmdline = ['--logger-log-level', 'compaction=debug', '--logger-log-level', 'repair=debug', '--smp', '2']
    servers = [await manager.server_add(config=cfg, cmdline=cmdline, property_file={"dc": "dc1", "rack": rack})
               for rack in ["r1", "r2", "r3"]]
    cql = manager.get_cql()
    await manager.disable_tablet_balancing()

    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} "
                                             "AND tablets = {'initial': 1}")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, v int, PRIMARY KEY (pk, ck)) "
                        "WITH tombstone_gc = {'mode': 'repair', 'propagation_delay_in_seconds': '1'}")
    table_id = await manager.get_table_id(ks, "test")
    scylla_path = await manager.server_get_exe(servers[0].server_id)

    await insert_range(cql, ks, 0, 100)
    await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.test (pk, ck, v) VALUES ({pk}, 1, 1)",
                                        consistency_level=ConsistencyLevel.ALL))
    await flush_all(manager, servers, ks)

    y, holders = servers[1], [servers[0], servers[2]]

    # Y misses the delete.
    await manager.server_stop_gracefully(y.server_id)
    await cql.run_async(SimpleStatement(f"DELETE FROM {ks}.test WHERE pk = {pk} AND ck = 1",
                                        consistency_level=ConsistencyLevel.QUORUM))
    await manager.server_start(y.server_id)
    cql = await reconnect_driver(manager)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # The holders merge R into T: their sstables keep T only.
    for s in holders:
        await manager.api.keyspace_compaction(s.ip_addr, ks, "test")
    await dump_r(manager, cql, ks, servers, hosts, scylla_path, "before-repair", pk)

    # repair_time - propagation_delay must be past T's deletion time. Repair caches the hints/batchlog
    # flush time (which becomes repair_time) for 60s, so wait that out.
    await asyncio.sleep(65)

    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]
    await inject_error_on(manager, "wait_after_prepare_sstables_for_incremental_repair", servers)
    before = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)
    repair = asyncio.create_task(manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all", incremental_mode='incremental'))
    for log, mark in zip(logs, marks):
        await log.wait_for('wait_after_prepare_sstables_for_incremental_repair: waiting for message', from_mark=mark)

    if read_during_repair:
        rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = {pk}",
                                                   consistency_level=ConsistencyLevel.ALL))
        logger.info(f"CL=ALL read during the repair window: {list(rows)}")
        await dump_r(manager, cql, ks, servers, hosts, scylla_path, "after-read-repair", pk)

    for s in servers:
        await manager.api.message_injection(s.ip_addr, "wait_after_prepare_sstables_for_incremental_repair")
    await repair
    await manager.api.quiesce_topology(servers[0].ip_addr)
    await inject_error_off(manager, "wait_after_prepare_sstables_for_incremental_repair", servers)
    after = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)
    logger.info(f"sstables_repaired_at {before} -> {after}, repair_time {await load_tablet_repair_time(cql, hosts[:1], table_id)}")
    await dump_r(manager, cql, ks, servers, hosts, scylla_path, "after-repair", pk)

    # Tombstone GC on the holders.
    for s in holders:
        await manager.api.keyspace_compaction(s.ip_addr, ks, "test")
    live = await dump_r(manager, cql, ks, servers, hosts, scylla_path, "after-gc", pk)

    for s in servers:
        log = await manager.server_open_log(s.server_id)
        lines = await log.grep(r'\[(?:Compact|Major) ' + ks + r'\.test [0-9a-f-]+\] Compact')
        logger.info(f"server {s.server_id} compactions: {parse_compaction_lineage(lines, ks)}")

    rows = await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.test WHERE pk = {pk} BYPASS CACHE",
                                               consistency_level=ConsistencyLevel.ALL))
    logger.info(f"live after gc={live} CL=ALL read={list(rows)}")
    assert not list(rows), f"deleted row resurrected: {list(rows)}"
