#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import psutil
import pytest
import time
from typing import Callable

from test.cluster.util import reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.storage.conftest import space_limited_servers
from test.cluster.storage.test_out_of_space_prevention import DISK_FILL_TARGET_RATIO, global_cmdline, random_content_file

logger = logging.getLogger(__name__)

# Pauses every reshape job right before it compacts, until it receives a
# message or is stopped.
RESHAPE_INJECTION = "reshape_compaction_group_before_compact"

# On boot, size-tiered compaction reshapes a bucket of at least
# max_threshold (32 by default) sstables of similar size.
FLUSHES = 32
ROWS_PER_FLUSH = 64


async def populate_offstrategy_table(manager: ScyllaClusterManager, cql, server) -> tuple[str, str]:
    """Create a vnodes table and flush FLUSHES sstables into it, each with
    its own ROWS_PER_FLUSH rows, so that the table has to be reshaped on
    the next boot and every sstable holds data no other sstable has. Return
    the names of its keyspace and table."""
    # Create the keyspace and table without the helper context managers:
    # they drop the table on exit, but by then the node may be down.
    ks = "test_ks"
    table = "test"
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'enabled': false}}")
    await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, v int) WITH compaction = {{'class': 'SizeTieredCompactionStrategy'}}")
    await manager.api.disable_autocompaction(server.ip_addr, ks, table)
    for flush in range(FLUSHES):
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.{table} (pk, v) VALUES ({pk}, {pk})")
                               for pk in range(flush * ROWS_PER_FLUSH, (flush + 1) * ROWS_PER_FLUSH)])
        await manager.api.keyspace_flush(server.ip_addr, ks, table)
    sstable_info = await manager.api.get_sstable_info(server.ip_addr, ks, table)
    assert len(sstable_info[0]['sstables']) == FLUSHES
    return ks, table


async def start_with_paused_reshape(manager: ScyllaClusterManager, server, log) -> tuple[asyncio.Task, int]:
    """Start the stopped node in a background task, with the reshape jobs
    paused, and wait until the first job pauses. Return the start task and a
    log mark from before the start. Fail early if the node stops starting
    before a job pauses."""
    await manager.server_update_config(server.server_id, "error_injections_at_startup", [RESHAPE_INJECTION])
    mark = await log.mark()
    start_task = asyncio.create_task(manager.server_start(server.server_id))

    wait_task = asyncio.create_task(manager.api.wait_for_injection_enter(
        server.ip_addr, RESHAPE_INJECTION, threshold=1, deadline=time.time() + 120))
    done, _ = await asyncio.wait({wait_task, start_task}, return_when=asyncio.FIRST_COMPLETED)
    if wait_task not in done:
        wait_task.cancel()
        await asyncio.gather(wait_task, return_exceptions=True)
        await start_task
        pytest.fail("The node started before a reshape job paused")
    wait_task.result()
    assert await log.grep("found SSTables that need reshape", from_mark=mark)
    return start_task, mark


async def check_all_rows(manager: ScyllaClusterManager, ks: str, table: str) -> None:
    """Check that every row written by populate_offstrategy_table() is
    readable, i.e. that every sstable was loaded."""
    await reconnect_driver(manager)
    cql = manager.get_cql()
    rows = await cql.run_async(f"SELECT pk, v FROM {ks}.{table}", all_pages=True)
    assert sorted(r.pk for r in rows) == list(range(FLUSHES * ROWS_PER_FLUSH))
    assert all(r.v == r.pk for r in rows)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_boot_continues_when_reshape_is_drained(manager: ScyllaClusterManager, volumes_factory: Callable) -> None:
    """
    Test that a node starts, with all of its sstables loaded, if out-of-space
    prevention drains the compaction manager while boot-time reshape runs.

    Unlike resharding, reshape is an optimization: the sstables that were not
    yet reshaped are still readable as they are, so the node must load them
    and start.

    Scenario:
      - Start a node and flush an off-strategy layout into a table.
      - Restart the node, which reshapes the table on boot, and pause the
        reshape job.
      - Create a big file to push the node over the critical disk utilization
        level, so that the disk space monitor drains the compaction manager
        and stops the paused job.
      - Expect the node to start and every row to be readable.
    """
    async with space_limited_servers(manager, volumes_factory, ["20M"], cmdline=[*global_cmdline, "--smp", "1"]) as servers:
        server = servers[0]
        cql, _ = await manager.get_ready_cql(servers)
        workdir = await manager.server_get_workdir(server.server_id)
        log = await manager.server_open_log(server.server_id)
        ks, table = await populate_offstrategy_table(manager, cql, server)

        logger.info("Restart the node and pause the reshape job of the table")
        await manager.server_stop_gracefully(server.server_id)
        start_task, mark = await start_with_paused_reshape(manager, server, log)
        try:
            logger.info("Create a big file on the node to reach critical disk utilization level")
            disk_info = psutil.disk_usage(workdir)
            with random_content_file(workdir, int(disk_info.total*DISK_FILL_TARGET_RATIO) - disk_info.used):
                # The drain stops the paused job, which releases it, and the
                # compaction notices the stop request before it writes anything.
                await start_task
        finally:
            if not start_task.done():
                start_task.cancel()

        assert await log.grep(f"Compaction for {ks}/{table} was stopped due to: drain", from_mark=mark)
        assert await log.grep("had reshape successfully aborted", from_mark=mark)
        assert not await log.grep("Startup failed", from_mark=mark)
        await check_all_rows(manager, ks, table)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_boot_continues_when_reshape_is_stopped(manager: ScyllaClusterManager) -> None:
    """
    Test that a node starts, with all of its sstables loaded, if a user stops
    boot-time reshape through the stop compaction API (nodetool stop RESHAPE).

    Same as test_boot_continues_when_reshape_is_drained, but a user stops the
    reshape job instead of out-of-space prevention draining the compaction
    manager.
    """
    server = await manager.server_add(cmdline=["--smp", "1"])
    cql, _ = await manager.get_ready_cql([server])
    log = await manager.server_open_log(server.server_id)
    ks, table = await populate_offstrategy_table(manager, cql, server)

    logger.info("Restart the node and pause the reshape job of the table")
    await manager.server_stop_gracefully(server.server_id)
    start_task, mark = await start_with_paused_reshape(manager, server, log)
    try:
        logger.info("Stop the reshape through the API")
        # The stop releases the paused job, and the compaction notices the
        # stop request before it writes anything.
        await manager.api.stop_compaction(server.ip_addr, "RESHAPE")
        await start_task
    finally:
        if not start_task.done():
            start_task.cancel()

    assert await log.grep(f"Compaction for {ks}/{table} was stopped due to: user request", from_mark=mark)
    assert await log.grep("had reshape successfully aborted", from_mark=mark)
    assert not await log.grep("Startup failed", from_mark=mark)
    await check_all_rows(manager, ks, table)
