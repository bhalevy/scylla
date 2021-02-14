/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "querier.hh"
#include "service/priority_manager.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/random_utils.hh"
#include "db/config.hh"

#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

class dummy_result_builder {
    std::optional<dht::decorated_key> _dk;
    std::optional<clustering_key_prefix> _ck;

public:
    dummy_result_builder()
        : _dk({dht::token(), partition_key::make_empty()})
        , _ck(clustering_key_prefix::make_empty()) {
    }

    void consume_new_partition(const dht::decorated_key& dk) {
        _dk = dk;
        _ck = {};
    }
    void consume(tombstone t) {
    }
    stop_iteration consume(static_row&& sr, tombstone t, bool is_live) {
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_live) {
        _ck = cr.key();
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone&& rt) {
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }
    std::pair<std::optional<dht::decorated_key>, std::optional<clustering_key_prefix>> consume_end_of_stream() {
        return {std::move(_dk), std::move(_ck)};
    }
};

class test_querier_cache {
public:
    using bound = range_bound<std::size_t>;

    static const size_t max_reader_buffer_size = 8 * 1024;

private:
    // Expected value of the above counters, updated by this.
    unsigned _expected_factory_invoked{};
    query::querier_cache::stats _expected_stats;

    simple_schema _s;
    reader_concurrency_semaphore _sem;
    query::querier_cache _cache;
    const std::vector<mutation> _mutations;
    const mutation_source _mutation_source;

    static sstring make_value(size_t i) {
        return format("value{:010d}", i);
    }

    static std::vector<mutation> make_mutations(simple_schema& s, const noncopyable_function<sstring(size_t)>& make_value) {
        std::vector<mutation> mutations;
        mutations.reserve(10);

        for (uint32_t i = 0; i != 10; ++i) {
            auto mut = mutation(s.schema(), s.make_pkey(i));

            s.add_static_row(mut, "-");
            s.add_row(mut, s.make_ckey(0), make_value(0));
            s.add_row(mut, s.make_ckey(1), make_value(1));
            s.add_row(mut, s.make_ckey(2), make_value(2));
            s.add_row(mut, s.make_ckey(3), make_value(3));

            mutations.emplace_back(std::move(mut));
        }

        boost::sort(mutations, [] (const mutation& a, const mutation& b) {
            return a.decorated_key().tri_compare(*a.schema(), b.decorated_key()) < 0;
        });

        return mutations;
    }

    template <typename Querier>
    Querier make_querier(const dht::partition_range& range) {
        return Querier(_mutation_source,
            _s.schema(),
            _sem.make_permit(_s.schema().get(), "make-querier"),
            range,
            _s.schema()->full_slice(),
            service::get_local_sstable_query_read_priority(),
            nullptr);
    }

    static utils::UUID make_cache_key(unsigned key) {
        return utils::UUID{key, 1};
    }

    const dht::decorated_key* find_key(const dht::partition_range& range, unsigned partition_offset) const {
        const auto& s = *_s.schema();
        const auto less_cmp = dht::ring_position_less_comparator(s);

        const auto begin = _mutations.begin();
        const auto end = _mutations.end();
        const auto start_position = range.start() ?
            dht::ring_position_view::for_range_start(range) :
            dht::ring_position_view(_mutations.begin()->decorated_key());

        const auto it = std::lower_bound(begin, end, start_position, [&] (const mutation& m, const dht::ring_position_view& k) {
            return less_cmp(m.ring_position(), k);
        });

        if (it == end) {
            return nullptr;
        }

        const auto dist = std::distance(it, end);
        auto& mut = *(partition_offset >= dist ? it + dist : it + partition_offset);
        return &mut.decorated_key();
    }

public:
    struct entry_info {
        reader_permit permit;
        unsigned key;
        dht::partition_range original_range;
        query::partition_slice original_slice;
        uint64_t row_limit;

        dht::partition_range expected_range;
        query::partition_slice expected_slice;
    };

    test_querier_cache(const noncopyable_function<sstring(size_t)>& external_make_value, std::chrono::seconds entry_ttl = 24h, ssize_t max_memory = std::numeric_limits<ssize_t>::max())
        : _sem(std::numeric_limits<int>::max(), max_memory, "test_querier_cache")
        , _cache(entry_ttl)
        , _mutations(make_mutations(_s, external_make_value))
        , _mutation_source([this] (schema_ptr, reader_permit permit, const dht::partition_range& range) {
            auto rd = flat_mutation_reader_from_mutations(std::move(permit), _mutations, range);
            rd.set_max_buffer_size(max_reader_buffer_size);
            return rd;
        }) {
    }

    explicit test_querier_cache(std::chrono::seconds entry_ttl = 24h)
        : test_querier_cache(test_querier_cache::make_value, entry_ttl) {
    }

    ~test_querier_cache() {
        _cache.stop().get();
        _sem.stop().get();
    }

    const simple_schema& get_simple_schema() const {
        return _s;
    }

    simple_schema& get_simple_schema() {
        return _s;
    }

    const schema_ptr get_schema() const {
        return _s.schema();
    }

    reader_concurrency_semaphore& get_semaphore() {
        return _sem;
    }

    dht::partition_range make_partition_range(bound begin, bound end) const {
        return dht::partition_range::make({_mutations.at(begin.value()).decorated_key(), begin.is_inclusive()},
                {_mutations.at(end.value()).decorated_key(), end.is_inclusive()});
    }

    dht::partition_range make_singular_partition_range(std::size_t i) const {
        return dht::partition_range::make_singular(_mutations.at(i).decorated_key());
    }

    dht::partition_range make_default_partition_range() const {
        return make_partition_range({0, true}, {_mutations.size() - 1, true});
    }

    const query::partition_slice& make_default_slice() const {
        return _s.schema()->full_slice();
    }

    template <typename Querier>
    entry_info produce_first_page_and_save_querier(unsigned key, const dht::partition_range& range,
            const query::partition_slice& slice, uint64_t row_limit) {
        const auto cache_key = make_cache_key(key);

        auto querier = make_querier<Querier>(range);
        auto dk_ck = querier.consume_page(dummy_result_builder{}, row_limit, std::numeric_limits<uint32_t>::max(),
                gc_clock::now(), db::no_timeout, query::max_result_size(std::numeric_limits<uint64_t>::max())).get0();
        auto&& dk = dk_ck.first;
        auto&& ck = dk_ck.second;
        auto permit = querier.permit();
        _cache.insert(cache_key, std::move(querier), nullptr);

        // Either no keys at all (nothing read) or at least partition key.
        BOOST_REQUIRE((dk && ck) || !ck);

        // Check that the read stopped at the correct position.
        // There are 5 rows in each mutation (1 static + 4 clustering).
        const auto* expected_key = find_key(range, row_limit / 5);
        if (!expected_key) {
            BOOST_REQUIRE(!dk);
            BOOST_REQUIRE(!ck);
        } else {
            BOOST_REQUIRE(dk->equal(*_s.schema(), *expected_key));
        }

        auto expected_range = [&] {
            if (range.is_singular() || !dk) {
                return range;
            }

            return dht::partition_range(dht::partition_range::bound(*dk, true), range.end());
        }();

        auto expected_slice = [&] {
            if (!ck) {
                return slice;
            }

            auto expected_slice = slice;
            auto cr = query::clustering_range::make_starting_with({*ck, false});
            expected_slice.set_range(*_s.schema(), dk->key(), {std::move(cr)});

            return expected_slice;
        }();

        return {std::move(permit), key, std::move(range), std::move(slice), row_limit, std::move(expected_range), std::move(expected_slice)};
    }

    entry_info produce_first_page_and_save_data_querier(unsigned key, const dht::partition_range& range,
            const query::partition_slice& slice, uint64_t row_limit = 5) {
        return produce_first_page_and_save_querier<query::data_querier>(key, range, slice, row_limit);
    }

    entry_info produce_first_page_and_save_data_querier(unsigned key, const dht::partition_range& range, uint64_t row_limit = 5) {
        return produce_first_page_and_save_data_querier(key, range, make_default_slice(), row_limit);
    }

    // Singular overload
    entry_info produce_first_page_and_save_data_querier(unsigned key, std::size_t i, uint64_t row_limit = 5) {
        return produce_first_page_and_save_data_querier(key, make_singular_partition_range(i), _s.schema()->full_slice(), row_limit);
    }

    // Use the whole range
    entry_info produce_first_page_and_save_data_querier(unsigned key) {
        return produce_first_page_and_save_data_querier(key, make_default_partition_range(), _s.schema()->full_slice());
    }

    // For tests testing just one insert-lookup.
    entry_info produce_first_page_and_save_data_querier() {
        return produce_first_page_and_save_data_querier(1);
    }

    entry_info produce_first_page_and_save_mutation_querier(unsigned key, const dht::partition_range& range,
            const query::partition_slice& slice, uint64_t row_limit = 5) {
        return produce_first_page_and_save_querier<query::mutation_querier>(key, range, slice, row_limit);
    }

    entry_info produce_first_page_and_save_mutation_querier(unsigned key, const dht::partition_range& range, uint64_t row_limit = 5) {
        return produce_first_page_and_save_mutation_querier(key, range, make_default_slice(), row_limit);
    }

    // Singular overload
    entry_info produce_first_page_and_save_mutation_querier(unsigned key, std::size_t i, uint64_t row_limit = 5) {
        return produce_first_page_and_save_mutation_querier(key, make_singular_partition_range(i), _s.schema()->full_slice(), row_limit);
    }

    // Use the whole range
    entry_info produce_first_page_and_save_mutation_querier(unsigned key) {
        return produce_first_page_and_save_mutation_querier(key, make_default_partition_range(), _s.schema()->full_slice());
    }

    // For tests testing just one insert-lookup.
    entry_info produce_first_page_and_save_mutation_querier() {
        return produce_first_page_and_save_mutation_querier(1);
    }

    test_querier_cache& assert_cache_lookup_data_querier(unsigned lookup_key,
            const schema& lookup_schema,
            const dht::partition_range& lookup_range,
            const query::partition_slice& lookup_slice) {

        auto querier_opt = _cache.lookup_data_querier(make_cache_key(lookup_key), lookup_schema, lookup_range, lookup_slice, nullptr);
        if (querier_opt) {
            querier_opt->close().get();
        }
        BOOST_REQUIRE_EQUAL(_cache.get_stats().lookups, ++_expected_stats.lookups);
        return *this;
    }

    test_querier_cache& assert_cache_lookup_mutation_querier(unsigned lookup_key,
            const schema& lookup_schema,
            const dht::partition_range& lookup_range,
            const query::partition_slice& lookup_slice) {

        auto querier_opt = _cache.lookup_mutation_querier(make_cache_key(lookup_key), lookup_schema, lookup_range, lookup_slice, nullptr);
        if (querier_opt) {
            querier_opt->close().get();
        }
        BOOST_REQUIRE_EQUAL(_cache.get_stats().lookups, ++_expected_stats.lookups);
        return *this;
    }

    test_querier_cache& evict_all_for_table() {
        _cache.evict_all_for_table(get_schema()->id()).get();
        return *this;
    }

    test_querier_cache& no_misses() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().misses, _expected_stats.misses);
        return *this;
    }

    test_querier_cache& misses() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().misses, ++_expected_stats.misses);
        return *this;
    }

    test_querier_cache& no_drops() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().drops, _expected_stats.drops);
        return *this;
    }

    test_querier_cache& drops() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().drops, ++_expected_stats.drops);
        return *this;
    }

    test_querier_cache& no_evictions() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().time_based_evictions, _expected_stats.time_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().resource_based_evictions, _expected_stats.resource_based_evictions);
        return *this;
    }

    test_querier_cache& time_based_evictions() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().time_based_evictions, ++_expected_stats.time_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().resource_based_evictions, _expected_stats.resource_based_evictions);
        return *this;
    }

    test_querier_cache& resource_based_evictions() {
        BOOST_REQUIRE_EQUAL(_cache.get_stats().time_based_evictions, _expected_stats.time_based_evictions);
        BOOST_REQUIRE_EQUAL(_cache.get_stats().resource_based_evictions, ++_expected_stats.resource_based_evictions);
        return *this;
    }
};

SEASTAR_THREAD_TEST_CASE(lookup_with_wrong_key_misses) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_data_querier();
    t.assert_cache_lookup_data_querier(90, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_data_querier_as_mutation_querier_misses) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_data_querier();
    t.assert_cache_lookup_mutation_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .misses()
        .no_drops()
        .no_evictions();

    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_mutation_querier_as_data_querier_misses) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_mutation_querier();
    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .misses()
        .no_drops()
        .no_evictions();

    t.assert_cache_lookup_mutation_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(data_and_mutation_querier_can_coexist) {
    test_querier_cache t;

    const auto data_entry = t.produce_first_page_and_save_data_querier(1);
    const auto mutation_entry = t.produce_first_page_and_save_mutation_querier(1);

    t.assert_cache_lookup_data_querier(data_entry.key, *t.get_schema(), data_entry.expected_range, data_entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();

    t.assert_cache_lookup_mutation_querier(mutation_entry.key, *t.get_schema(), mutation_entry.expected_range, mutation_entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();
}

/*
 * Range matching tests
 */

SEASTAR_THREAD_TEST_CASE(singular_range_lookup_with_stop_at_clustering_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_data_querier(1, t.make_singular_partition_range(1), 2);
    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(singular_range_lookup_with_stop_at_static_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_data_querier(1, t.make_singular_partition_range(1), 1);
    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_with_stop_at_clustering_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_data_querier(1, t.make_partition_range({1, true}, {3, false}), 3);
    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_with_stop_at_static_row) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_data_querier(1, t.make_partition_range({1, true}, {3, false}), 1);
    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .no_misses()
        .no_drops()
        .no_evictions();
}

/*
 * Drop tests
 */

SEASTAR_THREAD_TEST_CASE(lookup_with_original_range_drops) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_data_querier(1);
    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.original_range, entry.expected_slice)
        .no_misses()
        .drops()
        .no_evictions();

}

SEASTAR_THREAD_TEST_CASE(lookup_with_wrong_slice_drops) {
    test_querier_cache t;

    // Swap slices for different clustering keys.
    const auto entry1 = t.produce_first_page_and_save_data_querier(1, t.make_partition_range({1, false}, {3, true}), 3);
    const auto entry2 = t.produce_first_page_and_save_data_querier(2, t.make_partition_range({1, false}, {3, true}), 4);
    t.assert_cache_lookup_data_querier(entry1.key, *t.get_schema(), entry1.expected_range, entry2.expected_slice)
        .no_misses()
        .drops()
        .no_evictions();
    t.assert_cache_lookup_data_querier(entry2.key, *t.get_schema(), entry2.expected_range, entry1.expected_slice)
        .no_misses()
        .drops()
        .no_evictions();

    // Wrong slice.
    const auto entry3 = t.produce_first_page_and_save_data_querier(3);
    t.assert_cache_lookup_data_querier(entry3.key, *t.get_schema(), entry3.expected_range, t.get_schema()->full_slice())
        .no_misses()
        .drops()
        .no_evictions();

    // Swap slices for stopped at clustering/static row.
    const auto entry4 = t.produce_first_page_and_save_data_querier(4, t.make_partition_range({1, false}, {3, true}), 1);
    const auto entry5 = t.produce_first_page_and_save_data_querier(5, t.make_partition_range({1, false}, {3, true}), 2);
    t.assert_cache_lookup_data_querier(entry4.key, *t.get_schema(), entry4.expected_range, entry5.expected_slice)
        .no_misses()
        .drops()
        .no_evictions();
    t.assert_cache_lookup_data_querier(entry5.key, *t.get_schema(), entry5.expected_range, entry4.expected_slice)
        .no_misses()
        .drops()
        .no_evictions();
}

SEASTAR_THREAD_TEST_CASE(lookup_with_different_schema_version_drops) {
    test_querier_cache t;

    auto new_schema = schema_builder(t.get_schema()).with_column("v1", utf8_type).build();

    const auto entry = t.produce_first_page_and_save_data_querier();
    t.assert_cache_lookup_data_querier(entry.key, *new_schema, entry.expected_range, entry.expected_slice)
        .no_misses()
        .drops()
        .no_evictions();
}

/*
 * Eviction tests
 */

SEASTAR_THREAD_TEST_CASE(test_time_based_cache_eviction) {
    test_querier_cache t(1s);

    const auto entry1 = t.produce_first_page_and_save_data_querier(1);

    seastar::sleep(500ms).get();

    const auto entry2 = t.produce_first_page_and_save_data_querier(2);

    seastar::sleep(700ms).get();

    t.assert_cache_lookup_data_querier(entry1.key, *t.get_schema(), entry1.expected_range, entry1.expected_slice)
        .misses()
        .no_drops()
        .time_based_evictions();

    seastar::sleep(700ms).get();

    t.assert_cache_lookup_data_querier(entry2.key, *t.get_schema(), entry2.expected_range, entry2.expected_slice)
        .misses()
        .no_drops()
        .time_based_evictions();

    // There should be no inactive reads, the querier_cache should unregister
    // the expired queriers.
    BOOST_REQUIRE_EQUAL(t.get_semaphore().get_stats().inactive_reads, 0);
}

sstring make_string_blob(size_t size) {
    const char* const letters = "abcdefghijklmnoqprsuvwxyz";
    auto& re = seastar::testing::local_random_engine;
    std::uniform_int_distribution<size_t> dist(0, 25);

    sstring s;
    s.resize(size);

    for (size_t i = 0; i < size; ++i) {
        s[i] = letters[dist(re)];
    }

    return s;
}

SEASTAR_THREAD_TEST_CASE(test_memory_based_cache_eviction) {
    auto cache_size = 1 << 20;
    test_querier_cache t([] (size_t) {
        const size_t blob_size = 1 << 10; // 1K
        return make_string_blob(blob_size);
    }, 24h, cache_size);

    size_t i = 0;
    auto entry = t.produce_first_page_and_save_data_querier(i++);
    auto& sem = entry.permit.semaphore();
    const auto entry_size = entry.permit.consumed_resources().memory;

    // Fill the cache but don't overflow.
    while (sem.available_resources().memory > entry_size) {
        t.produce_first_page_and_save_data_querier(i++);
    }

    const auto pop_before = t.get_semaphore().get_stats().inactive_reads;

    // Should overflow the limit and thus be evicted instantly.
    entry = t.produce_first_page_and_save_data_querier(i++);

    t.assert_cache_lookup_data_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .resource_based_evictions()
        .misses()
        .no_drops();

    // Since the last insert should have evicted an existing entry, we should
    // have the same number of registered inactive reads.
    BOOST_REQUIRE_EQUAL(t.get_semaphore().get_stats().inactive_reads, pop_before);
}

SEASTAR_THREAD_TEST_CASE(test_resources_based_cache_eviction) {
    auto db_cfg_ptr = make_shared<db::config>();
    auto& db_cfg = *db_cfg_ptr;

    db_cfg.enable_cache(false);
    db_cfg.enable_commitlog(false);

    do_with_cql_env([] (cql_test_env& env) {
        using namespace std::chrono_literals;

        auto& db = env.local_db();

        db.set_querier_cache_entry_ttl(24h);

        try {
            db.find_keyspace("querier_cache");
            env.execute_cql("drop keyspace querier_cache;").get();
        } catch (const no_such_keyspace&) {
            // expected
        }

        env.execute_cql("CREATE KEYSPACE querier_cache WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").get();
        env.execute_cql("CREATE TABLE querier_cache.test (pk int, ck int, value int, primary key (pk, ck));").get();

        env.require_table_exists("querier_cache", "test").get();

        auto insert_id = env.prepare("INSERT INTO querier_cache.test (pk, ck, value) VALUES (?, ?, ?);").get0();
        auto pk = cql3::raw_value::make_value(serialized(0));
        for (int i = 0; i < 100; ++i) {
            auto ck = cql3::raw_value::make_value(serialized(i));
            env.execute_prepared(insert_id, {{pk, ck, ck}}).get();
        }

        env.require_table_exists("querier_cache", "test").get();

        auto& cf = db.find_column_family("querier_cache", "test");
        auto s = cf.schema();

        cf.flush().get();
        auto slice = s->full_slice();
        slice.options.set<query::partition_slice::option::allow_short_read>();

        auto cmd1 = query::read_command(s->id(),
                s->version(),
                slice,
                1,
                gc_clock::now(),
                std::nullopt,
                1,
                utils::make_random_uuid(),
                query::is_first_page::yes,
                query::max_result_size(1024 * 1024),
                0);

        // Should save the querier in cache.
        db.query_mutations(s,
                cmd1,
                query::full_partition_range,
                nullptr,
                db::no_timeout).get();

        auto& semaphore = db.get_reader_concurrency_semaphore();

        BOOST_CHECK_EQUAL(db.get_querier_cache_stats().resource_based_evictions, 0);

        // Drain all resources of the semaphore
        const auto available_resources = semaphore.available_resources();
        semaphore.consume(available_resources);
        auto release_resources = defer([&semaphore, available_resources] {
            semaphore.signal(available_resources);
        });

        auto cmd2 = query::read_command(s->id(),
                s->version(),
                slice,
                1,
                gc_clock::now(),
                std::nullopt,
                1,
                utils::make_random_uuid(),
                query::is_first_page::no,
                query::max_result_size(1024 * 1024),
                0);

        // Should evict the already cached querier.
        db.query_mutations(s,
                cmd2,
                query::full_partition_range,
                nullptr,
                db::no_timeout).get();

        BOOST_CHECK_EQUAL(db.get_querier_cache_stats().resource_based_evictions, 1);

        // We want to read the entire partition so that the querier
        // is not saved at the end and thus ensure it is destroyed.
        // We cannot leave scope with the querier still in the cache
        // as that sadly leads to use-after-free as the database's
        // resource_concurrency_semaphore will be destroyed before some
        // of the tracked buffers.
        cmd2.set_row_limit(query::max_rows);
        cmd2.partition_limit = query::max_partitions;
        cmd2.max_result_size.emplace(query::result_memory_limiter::unlimited_result_size);
        db.query_mutations(s,
                cmd2,
                query::full_partition_range,
                nullptr,
                db::no_timeout).get();
        return make_ready_future<>();
    }, std::move(db_cfg_ptr)).get();
}

SEASTAR_THREAD_TEST_CASE(test_evict_all_for_table) {
    test_querier_cache t;

    const auto entry = t.produce_first_page_and_save_mutation_querier();

    t.evict_all_for_table();
    t.assert_cache_lookup_mutation_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .misses()
        .no_drops()
        .no_evictions();

    // Check that the querier was removed from the semaphore too.
    BOOST_CHECK(!t.get_semaphore().try_evict_one_inactive_read());
}

SEASTAR_THREAD_TEST_CASE(test_immediate_evict_on_insert) {
    test_querier_cache t;

    auto& sem = t.get_semaphore();
    auto permit1 = sem.make_permit(t.get_schema().get(), get_name());
    auto permit2 = sem.make_permit(t.get_schema().get(), get_name());

    permit1.wait_admission(0, db::no_timeout).get();

    auto resources = permit1.consume_resources(reader_resources(sem.available_resources().count, 0));

    BOOST_CHECK_EQUAL(sem.available_resources().count, 0);

    auto fut = permit2.wait_admission(1, db::no_timeout);

    BOOST_CHECK_EQUAL(sem.waiters(), 1);

    const auto entry = t.produce_first_page_and_save_mutation_querier();
    t.assert_cache_lookup_mutation_querier(entry.key, *t.get_schema(), entry.expected_range, entry.expected_slice)
        .misses()
        .no_drops()
        .resource_based_evictions();

    resources.reset();

    fut.get();
}

SEASTAR_THREAD_TEST_CASE(test_unique_inactive_read_handle) {
    reader_concurrency_semaphore sem1(reader_concurrency_semaphore::no_limits{}, "sem1");
    auto stop_sem1 = deferred_stop(sem1);
    reader_concurrency_semaphore sem2(reader_concurrency_semaphore::no_limits{}, ""); // to see the message for an unnamed semaphore
    auto stop_sem2 = deferred_stop(sem2);

    auto schema = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("v", int32_type)
        .build();

    auto sem1_h1 = sem1.register_inactive_read(make_empty_flat_reader(schema, sem1.make_permit(schema.get(), get_name())));
    auto sem2_h1 = sem2.register_inactive_read(make_empty_flat_reader(schema, sem2.make_permit(schema.get(), get_name())));

    // Sanity check that lookup still works with empty handle.
    BOOST_REQUIRE(!sem1.unregister_inactive_read(reader_concurrency_semaphore::inactive_read_handle{}));

    set_abort_on_internal_error(false);
    auto reset_on_internal_abort = defer([] {
        set_abort_on_internal_error(true);
    });
    BOOST_REQUIRE_THROW(sem1.unregister_inactive_read(std::move(sem2_h1)), std::runtime_error);
    BOOST_REQUIRE_THROW(sem2.unregister_inactive_read(std::move(sem1_h1)), std::runtime_error);
}
