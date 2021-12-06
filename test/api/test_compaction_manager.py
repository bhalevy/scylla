# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

import sys

from requests import HTTPError

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table

def test_compaction_manager_stop_compaction(cql, api):
    resp = api.send("POST", "compaction_manager/stop_compaction", { "type": "COMPACTION" })
    resp.raise_for_status()

def test_compaction_manager_stop_keyspace_compaction(cql, api, test_keyspace):
    resp = api.send("POST", f"compaction_manager/stop_keyspace_compaction/{test_keyspace}", { "type": "RESHAPE" })
    resp.raise_for_status()

    # non-existing keyspace
    resp = api.send("POST", f"compaction_manager/stop_keyspace_compaction/{test_keyspace}XXX", { "type": "RESHAPE" })
    try:
        resp.raise_for_status()
    except HTTPError:
        pass

def test_compaction_manager_stop_keyspace_compaction_tables(cql, api, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t0:
        test_tables = [t0 if not '.' in t0 else t0.split('.')[1]]
        resp = api.send("POST", f"compaction_manager/stop_keyspace_compaction/{test_keyspace}", { "tables": f"{test_tables[0]}", "type": "CLEANUP" })
        resp.raise_for_status()

        # non-existing table
        resp = api.send("POST", f"compaction_manager/stop_keyspace_compaction/{test_keyspace}", { "tables": "foo", "type": "CLEANUP" })
        try:
            resp.raise_for_status()
        except HTTPError:
            pass

        # multiple tables
        with new_test_table(cql, test_keyspace, "b int, PRIMARY KEY (b)") as t1:
            test_tables += [t1 if not '.' in t1 else t1.split('.')[1]]
            resp = api.send("POST", f"compaction_manager/stop_keyspace_compaction/{test_keyspace}", { "tables": f"{test_tables[0]},{test_tables[1]}", "type": "CLEANUP" })
            resp.raise_for_status()

            # mixed existing and non-existing tables
            resp = api.send("POST", f"compaction_manager/stop_keyspace_compaction/{test_keyspace}", { "tables": f"{test_tables[1]},foo", "type": "CLEANUP" })
            try:
                resp.raise_for_status()
            except HTTPError:
                pass
