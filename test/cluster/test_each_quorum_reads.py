#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
EACH_QUORUM reads contact a quorum of replicas in every datacenter.

The tests below are arranged so that a coordinator which merely counted
responses across the cluster (block_for = sum of the per-DC quorums) would
pass or fail differently from one that checks each datacenter.
"""
import asyncio
import logging

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import NoHostAvailable
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.pylib.internal_types import ServerInfo
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import create_new_test_keyspace, new_test_keyspace

logger = logging.getLogger(__name__)

CONFIG = {'endpoint_snitch': 'GossipingPropertyFileSnitch', 'hinted_handoff_enabled': False}


def connect_to(manager: ScyllaClusterManager, server: ServerInfo):
    """A session pinned to one node, so that node is the coordinator."""
    return manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()


def unavailable_error(exc: Exception) -> Unavailable:
    """The driver may hand back the coordinator's Unavailable directly or
    wrapped in NoHostAvailable, depending on its retry decision."""
    if isinstance(exc, Unavailable):
        return exc
    assert isinstance(exc, NoHostAvailable), exc
    inner = list(exc.errors.values())
    assert len(inner) == 1 and isinstance(inner[0], Unavailable), exc
    return inner[0]


async def test_each_quorum_read_needs_a_quorum_in_every_dc(manager: ScyllaClusterManager):
    """
    Two datacenters at RF=3, so block_for = 2 + 2 = 4. With two dc1 nodes
    down four replicas are still alive, which satisfies a cluster-wide
    count, but dc1 only has one: the read must fail with Unavailable
    reporting dc1's 2 required / 1 alive.

    The same cluster also checks that an EACH_QUORUM read sees a
    LOCAL_QUORUM write made in the other datacenter, that a write needing
    a read-before-write runs at EACH_QUORUM, and that range scans follow
    the same rule.
    """
    dc1 = await manager.servers_add(3, config=CONFIG, auto_rack_dc='dc1')
    dc2 = await manager.servers_add(3, config=CONFIG, auto_rack_dc='dc2')
    dc1_cql = connect_to(manager, dc1[0])
    dc2_cql = connect_to(manager, dc2[0])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}") as ks:
        await dc1_cql.run_async(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY, v int, l list<int>)")

        def select(pk: int, cl=ConsistencyLevel.EACH_QUORUM):
            return SimpleStatement(f"SELECT v FROM {ks}.tbl WHERE pk = {pk}", consistency_level=cl)

        logger.info("A LOCAL_QUORUM write in dc1 is seen by an EACH_QUORUM read coordinated in dc2")
        await dc1_cql.run_async(SimpleStatement(f"INSERT INTO {ks}.tbl (pk, v) VALUES (1, 10)", consistency_level=ConsistencyLevel.LOCAL_QUORUM))
        assert [r.v for r in await dc2_cql.run_async(select(1))] == [10]

        logger.info("A write that needs a read-before-write runs at EACH_QUORUM")
        await dc1_cql.run_async(SimpleStatement(f"UPDATE {ks}.tbl SET l = [1, 2, 3] WHERE pk = 1", consistency_level=ConsistencyLevel.EACH_QUORUM))
        await dc1_cql.run_async(SimpleStatement(f"UPDATE {ks}.tbl SET l[1] = 20 WHERE pk = 1", consistency_level=ConsistencyLevel.EACH_QUORUM))
        assert [r.l for r in await dc2_cql.run_async(SimpleStatement(f"SELECT l FROM {ks}.tbl WHERE pk = 1", consistency_level=ConsistencyLevel.EACH_QUORUM))] == [[1, 20, 3]]

        logger.info("One dc1 node down: each datacenter still has a quorum")
        await manager.server_stop_gracefully(dc1[1].server_id)
        assert [r.v for r in await dc2_cql.run_async(select(1))] == [10]
        scan = SimpleStatement(f"SELECT pk FROM {ks}.tbl", consistency_level=ConsistencyLevel.EACH_QUORUM)
        assert [r.pk for r in await dc2_cql.run_async(scan)] == [1]

        logger.info("Two dc1 nodes down: four replicas are alive, but dc1 has no quorum")
        await manager.server_stop_gracefully(dc1[2].server_id)
        for stmt in (select(1), scan):
            with pytest.raises((Unavailable, NoHostAvailable)) as exc_info:
                await dc2_cql.run_async(stmt)
            err = unavailable_error(exc_info.value)
            assert err.consistency == ConsistencyLevel.EACH_QUORUM
            assert (err.required_replicas, err.alive_replicas) == (2, 1), err

        logger.info("The other levels that only need dc2 still work, to show the cluster itself is fine")
        assert [r.v for r in await dc2_cql.run_async(select(1, ConsistencyLevel.LOCAL_QUORUM))] == [10]
        assert [r.v for r in await dc2_cql.run_async(select(1, ConsistencyLevel.QUORUM))] == [10]


async def test_each_quorum_read_waits_for_every_dc_quorum(manager: ScyllaClusterManager):
    """
    dc1 at RF=3 and dc2 at RF=2 (block_for = 2 + 2 = 4) and speculative
    retry ALWAYS, so with all five replicas alive the coordinator contacts
    all of them. One dc2 replica is held on the replica-side read path:
    the four other responses reach a cluster-wide count of 4, but dc2 has
    answered 1 of 2, so the read must not complete until the held replica
    is released.
    """
    dc1 = await manager.servers_add(3, config=CONFIG, auto_rack_dc='dc1')
    dc2 = await manager.servers_add(2, config=CONFIG, auto_rack_dc='dc2')
    cql = connect_to(manager, dc1[0])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 2}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY, v int) WITH speculative_retry = 'ALWAYS'")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.tbl (pk, v) VALUES (1, 10)", consistency_level=ConsistencyLevel.EACH_QUORUM))

        held = dc2[1]
        logger.info(f"Holding reads of the table on {held.ip_addr} (dc2)")
        await inject_error_one_shot(manager.api, held.ip_addr, 'storage_proxy::handle_read', parameters={'cf_name': 'tbl'})

        # The server-side timeout is raised so the held read is not timed out
        # under load; the driver's own default timeout must be raised with it.
        read = cql.run_async(SimpleStatement(f"SELECT v FROM {ks}.tbl WHERE pk = 1 USING TIMEOUT 60s", consistency_level=ConsistencyLevel.EACH_QUORUM), timeout=120)
        await manager.api.wait_for_injection_enter(held.ip_addr, 'storage_proxy::handle_read')

        logger.info("Every other replica has had time to answer; dc2 is still one short")
        await asyncio.sleep(2)
        assert not read.done(), "EACH_QUORUM read completed without a quorum from dc2"

        logger.info("Releasing the held replica completes the read")
        await manager.api.message_injection(held.ip_addr, 'storage_proxy::handle_read')
        assert [r.v for r in await read] == [10]


async def test_each_quorum_read_repairs_the_other_dc(manager: ScyllaClusterManager):
    """
    dc1 at RF=3, dc2 at RF=1. With hints disabled, a LOCAL_QUORUM write in
    dc1 while dc2's node is down never reaches dc2. An EACH_QUORUM read
    coordinated in dc2 must still return the write (dc1's quorum has it)
    and, through read repair, leave dc2's replica up to date, which a
    LOCAL_ONE read in dc2 then confirms.
    """
    dc1 = await manager.servers_add(3, config=CONFIG, auto_rack_dc='dc1')
    dc2 = await manager.servers_add(1, config=CONFIG, auto_rack_dc='dc2')
    dc1_cql = connect_to(manager, dc1[0])

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 1}") as ks:
        await dc1_cql.run_async(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY, v int) WITH speculative_retry = 'NONE'")

        await manager.server_stop_gracefully(dc2[0].server_id)
        await dc1_cql.run_async(SimpleStatement(f"INSERT INTO {ks}.tbl (pk, v) VALUES (1, 10)", consistency_level=ConsistencyLevel.LOCAL_QUORUM))
        await manager.server_start(dc2[0].server_id)
        await manager.servers_see_each_other(dc1 + dc2)
        # Need reconnecting after start/stop of nodes
        await manager.driver_connect()
        dc2_cql = connect_to(manager, dc2[0])

        logger.info("dc2's only replica missed the write")
        local = SimpleStatement(f"SELECT v FROM {ks}.tbl WHERE pk = 1", consistency_level=ConsistencyLevel.LOCAL_ONE)
        assert [r.v for r in await dc2_cql.run_async(local)] == []

        logger.info("An EACH_QUORUM read returns the write and repairs dc2")
        each_quorum = SimpleStatement(f"SELECT v FROM {ks}.tbl WHERE pk = 1", consistency_level=ConsistencyLevel.EACH_QUORUM)
        assert [r.v for r in await dc2_cql.run_async(each_quorum)] == [10]
        assert [r.v for r in await dc2_cql.run_async(local)] == [10]


async def test_each_quorum_read_on_simple_strategy_is_a_quorum(manager: ScyllaClusterManager):
    """
    Without datacenters to iterate over, EACH_QUORUM degrades to QUORUM on
    SimpleStrategy, for reads as it already does for writes: RF=3 reads
    succeed with one node down and fail with two.
    """
    servers = await manager.servers_add(3, config=CONFIG, auto_rack_dc='dc1')
    cql = connect_to(manager, servers[0])

    # The test ends with two of the three nodes down, when the keyspace can
    # no longer be dropped (no group0 majority), so no cleanup is scheduled.
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND tablets = {'enabled': false}")
    await cql.run_async(f"CREATE TABLE {ks}.tbl (pk int PRIMARY KEY, v int)")
    await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.tbl (pk, v) VALUES (1, 10)", consistency_level=ConsistencyLevel.EACH_QUORUM))
    select = SimpleStatement(f"SELECT v FROM {ks}.tbl WHERE pk = 1", consistency_level=ConsistencyLevel.EACH_QUORUM)

    await manager.server_stop_gracefully(servers[1].server_id)
    assert [r.v for r in await cql.run_async(select)] == [10]

    await manager.server_stop_gracefully(servers[2].server_id)
    with pytest.raises((Unavailable, NoHostAvailable)) as exc_info:
        await cql.run_async(select)
    err = unavailable_error(exc_info.value)
    assert (err.required_replicas, err.alive_replicas) == (2, 1), err
