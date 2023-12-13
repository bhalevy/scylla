import pytest
from util import config_value_context, new_test_keyspace, new_test_table
from cassandra_tests.porting import *

# Tests for the enable_create_table_with_compact_storage guardrail.
# Because this feature does not exist in Cassandra , *all* tests in this file are
# Scylla-only. Let's mark them all scylla_only with an autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

def test_create_table_with_compact_storage_default_config(cql, this_dc):
    ks_defs = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}"
    with new_test_keyspace(cql, ks_defs) as test_keyspace:
        try:
            with new_test_table(cql, test_keyspace, schema="p int PRIMARY KEY, v int", extra="WITH COMPACT STORAGE") as test_table:
                pytest.fail
        except InvalidRequest:
            # expected to throw InvalidRequest
            pass

@pytest.mark.parametrize("config_value", [False, True])
def test_create_table_with_compact_storage_config(cql, this_dc, config_value):
    with config_value_context(cql, 'enable_create_table_with_compact_storage', str(config_value).lower()):
        ks_defs = f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}"
        with new_test_keyspace(cql, ks_defs) as test_keyspace:
            try:
                with new_test_table(cql, test_keyspace, schema="p int PRIMARY KEY, v int", extra="WITH COMPACT STORAGE") as test_table:
                    if not config_value:
                        pytest.fail
            except InvalidRequest:
                # expected to throw InvalidRequest only when
                # enable_create_table_with_compact_storage=false
                if config_value:
                    pytest.fail
