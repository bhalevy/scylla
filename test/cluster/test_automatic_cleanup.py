#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import os
from shutil import rmtree
from test.cluster.conftest import manager
from test.pylib.manager_client import ManagerClient
from test.pylib.scylla_cluster import ReplaceConfig
from test.cluster.conftest import skip_mode
from test.cluster.util import new_test_keyspace
from test.pylib.util import wait_for_cql_and_get_hosts, wait_for_view

import pytest
import logging
import asyncio
import glob
import shutil

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.prepare_3_racks_cluster


@pytest.mark.asyncio
async def test_no_cleanup_when_unnecessary(request, manager: ManagerClient):
    """The test runs two bootstraps and checks that there is no cleanup in between.
       Then it runs a decommission and checks that cleanup runs automatically and then
       it runs one more decommission and checks that no cleanup runs again.
       Second part checks manual cleanup triggering. It adds a node. Triggers cleanup
       through the REST API, checks that is runs, decommissions a node and check that the
       cleanup did not run again.
    """
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack1"})
    await manager.server_add(property_file={"dc": "dc1", "rack": "rack2"})
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[4].server_id)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 4

    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[3].server_id)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0

    await manager.server_add(property_file={"dc": "dc1", "rack": "rack3"})
    servers = await manager.running_servers()
    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]
    await manager.api.client.post("/storage_service/cleanup_all", servers[0].ip_addr)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 3

    marks = [await log.mark() for log in logs]
    await manager.decommission_node(servers[3].server_id)
    matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
    assert sum(len(x) for x in matches) == 0


@pytest.mark.asyncio
@skip_mode('release', 'error injection is disabled in release mode')
async def test_cleanup_before_decommission(request, manager: ManagerClient):
   servers = await manager.running_servers()

   async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': false}") as ks:
      await manager.cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int)")
      keys = range(1000)
      await asyncio.gather(*[manager.cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

      logs = [await manager.server_open_log(srv.server_id) for srv in servers]
      marks = [await log.mark() for log in logs]
      await manager.server_add(property_file={"dc": "dc1", "rack": "rack1"})
      await manager.server_add(property_file={"dc": "dc1", "rack": "rack2"})
      matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
      assert sum(len(x) for x in matches) == 0

      await manager.api.enable_injection(servers[0].ip_addr, "cleanup_error", one_shot=False)

      servers = await manager.running_servers()
      marks = [await log.mark() for log in logs]
      await manager.decommission_node(servers[4].server_id)
      matches = [await log.grep("raft_topology - start cleanup", from_mark=mark) for log, mark in zip(logs, marks)]
      assert sum(len(x) for x in matches) == 4
