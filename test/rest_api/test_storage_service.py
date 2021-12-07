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

import pytest
import sys
import requests

# Use the util.py library from ../cql-pytest:
sys.path.insert(1, sys.path[0] + '/../cql-pytest')
from util import new_test_table

def test_storage_service_auto_compaction(cql, rest_api, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
        resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{test_keyspace}")
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/auto_compaction/{test_keyspace}")
        resp.raise_for_status()

        # non-existing keyspace
        resp = rest_api.send("POST", f"storage_service/auto_compaction/XXX")
        try:
            resp.raise_for_status()
            pytest.fail("Failed to raise exception")
        except requests.HTTPError as e:
            assert resp.status_code == requests.codes.bad_request, e

def test_storage_service_auto_compaction_table(cql, rest_api, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t:
        test_table = t if not '.' in t else t.split('.')[1]
        resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{test_keyspace}", { "cf": test_table })
        resp.raise_for_status()

        resp = rest_api.send("POST", f"storage_service/auto_compaction/{test_keyspace}", { "cf": test_table })
        resp.raise_for_status()

        # non-existing table
        resp = rest_api.send("POST", f"storage_service/auto_compaction/{test_keyspace}", { "cf": "XXX" })
        try:
            resp.raise_for_status()
            pytest.fail("Failed to raise exception")
        except requests.HTTPError as e:
            expected_status_code = requests.codes.bad_request
            assert resp.status_code == expected_status_code, e

def test_storage_service_auto_compaction_tables(cql, rest_api, test_keyspace):
    with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t0:
        test_tables = [t0 if not '.' in t0 else t0.split('.')[1]]
        with new_test_table(cql, test_keyspace, "a int, PRIMARY KEY (a)") as t1:
            test_tables += [t1 if not '.' in t1 else t1.split('.')[1]]
            resp = rest_api.send("DELETE", f"storage_service/auto_compaction/{test_keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            resp = rest_api.send("POST", f"storage_service/auto_compaction/{test_keyspace}", { "cf": f"{test_tables[0]},{test_tables[1]}" })
            resp.raise_for_status()

            # non-existing table
            resp = rest_api.send("POST", f"storage_service/auto_compaction/{test_keyspace}", { "cf": f"{test_tables[0]},XXX" })
            try:
                resp.raise_for_status()
                pytest.fail("Failed to raise exception")
            except requests.HTTPError as e:
                expected_status_code = requests.codes.bad_request
                assert resp.status_code == expected_status_code, e
