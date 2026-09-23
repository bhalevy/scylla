/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>
#include <seastar/core/thread.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/closeable.hh>

#include "mutation/mutation_fragment.hh"
#include "schema/schema_builder.hh"
#include "mutation/mutation_rebuilder.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "readers/from_mutations.hh"
#include "mutation_writer/multishard_writer.hh"
#include "mutation_writer/timestamp_based_splitting_writer.hh"
#include "mutation_writer/partition_based_splitting_writer.hh"
#include "mutation_writer/token_group_based_splitting_writer.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/log.hh"
#include "test/lib/test_utils.hh"

#include "readers/from_mutations.hh"
#include "readers/empty.hh"
#include "readers/generating.hh"
#include "readers/combined.hh"

BOOST_AUTO_TEST_SUITE(mutation_writer_test)

using namespace mutation_writer;

struct generate_error_tag { };
using generate_error = bool_class<generate_error_tag>;


constexpr unsigned many_partitions() {
    return
#ifndef SEASTAR_DEBUG
	300
#else
	10
#endif
	;
}

SEASTAR_TEST_CASE(test_multishard_writer) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto test_random_streams = [&e] (random_mutation_generator&& gen, size_t partition_nr, generate_error error = generate_error::no) {
            for (auto i = 0; i < 3; i++) {
                auto muts = gen(partition_nr);
                std::vector<size_t> shards_before(this_smp_shard_count(), 0);
                std::vector<size_t> shards_after(this_smp_shard_count(), 0);
                schema_ptr s = gen.schema();

                for (auto& m : muts) {
                    auto shard = s->get_sharder().shard_for_reads(m.token());
                    shards_before[shard]++;
                }
                auto source_reader = partition_nr > 0 ? make_mutation_reader_from_mutations(gen.schema(), make_reader_permit(e), muts) : make_empty_mutation_reader(s, make_reader_permit(e));
                auto close_source_reader = deferred_close(source_reader);
                auto& sharder = s->get_sharder();
                size_t partitions_received = distribute_reader_and_consume_on_shards(s, sharder,
                    std::move(source_reader),
                    [&sharder, &shards_after, error] (mutation_reader reader) mutable {
                        if (error) {
                          return reader.close().then([] {
                            return make_exception_future<>(std::runtime_error("Failed to write"));
                          });
                        }
                        return with_closeable(std::move(reader), [&sharder, &shards_after] (mutation_reader& reader) {
                          return repeat([&sharder, &shards_after, &reader] () mutable {
                            return reader().then([&sharder, &shards_after] (mutation_fragment_v2_opt mf_opt) mutable {
                                if (mf_opt) {
                                    if (mf_opt->is_partition_start()) {
                                        auto shard = sharder.shard_for_reads(mf_opt->as_partition_start().key().token());
                                        BOOST_REQUIRE_EQUAL(shard, this_shard_id());
                                        shards_after[shard]++;
                                    }
                                    return make_ready_future<stop_iteration>(stop_iteration::no);
                                } else {
                                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                                }
                            });
                          });
                        });
                    }
                ).get();
                BOOST_REQUIRE_EQUAL(partitions_received, partition_nr);
                BOOST_REQUIRE_EQUAL(shards_after, shards_before);
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), 0);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), 0);

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), 1);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), 1);

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), many_partitions());
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), many_partitions());

        try {
            test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::no), many_partitions(), generate_error::yes);
            BOOST_ASSERT(false);
        } catch (...) {
        }

        try {
            test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes, local_shard_only::no), many_partitions(), generate_error::yes);
            BOOST_ASSERT(false);
        } catch (...) {
        }
    });
}

SEASTAR_TEST_CASE(test_multishard_writer_producer_aborts) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        auto test_random_streams = [&e] (random_mutation_generator&& gen, size_t partition_nr, generate_error error = generate_error::no) {
            auto muts = gen(partition_nr);
            schema_ptr s = gen.schema();
            auto source_reader = partition_nr > 0 ? make_mutation_reader_from_mutations(s, make_reader_permit(e), muts) : make_empty_mutation_reader(s, make_reader_permit(e));
            auto close_source_reader = deferred_close(source_reader);
            int mf_produced = 0;
            auto get_next_mutation_fragment = [&source_reader, &mf_produced] () mutable {
                if (mf_produced++ > 800) {
                    return make_exception_future<mutation_fragment_v2_opt>(std::runtime_error("the producer failed"));
                } else {
                    return source_reader();
                }
            };
            auto& sharder = s->get_sharder();
            try {
                distribute_reader_and_consume_on_shards(s, sharder,
                    make_generating_reader(s, make_reader_permit(e), std::move(get_next_mutation_fragment)),
                    [&sharder, error] (mutation_reader reader) mutable {
                        if (error) {
                          return reader.close().then([] {
                            return make_exception_future<>(std::runtime_error("Failed to write"));
                          });
                        }
                        return with_closeable(std::move(reader), [&sharder] (mutation_reader& reader) {
                          return repeat([&sharder, &reader] () mutable {
                            return reader().then([&sharder] (mutation_fragment_v2_opt mf_opt) mutable {
                                if (mf_opt) {
                                    if (mf_opt->is_partition_start()) {
                                        auto shard = sharder.shard_for_reads(mf_opt->as_partition_start().key().token());
                                        BOOST_REQUIRE_EQUAL(shard, this_shard_id());
                                    }
                                    return make_ready_future<stop_iteration>(stop_iteration::no);
                                } else {
                                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                                }
                            });
                          });
                        });
                    }
                ).get();
            } catch (...) {
                // The distribute_reader_and_consume_on_shards is expected to fail and not block forever
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::yes), 1000, generate_error::no);
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no, local_shard_only::yes), 1000, generate_error::yes);
    });
}

namespace {

using classify_by_var_t = std::variant<classify_by_timestamp, classify_by_token_group>;

class test_bucket_writer {
public:
    class expected_exception : public std::exception {
    public:
        virtual const char* what() const noexcept override {
            return "expected_exception";
        }
    };

    class throw_point {
        size_t _throw_after = std::numeric_limits<size_t>::max();
        size_t _count = 0;
        bool _hit = false;

    public:
        throw_point() = default; // no throw
        explicit throw_point(size_t throw_after) : _throw_after(throw_after) { }

        void check_throw(std::source_location sl = std::source_location::current()) {
            if (++_count >= _throw_after) {
                _hit = true;
                testlog.debug("Throw point hit after {} from {} @ {}:{}", _throw_after, sl.function_name(), sl.file_name(), sl.line());
                throw(expected_exception());
            }
        }

        size_t throw_after() const {
            return _throw_after;
        }

        bool check_hit_and_advance() {
            ++_throw_after;
            _count = 0;
            return std::exchange(_hit, false);
        }
    };

private:
    schema_ptr _schema;
    reader_permit _permit;
    classify_by_var_t _classify;
    std::unordered_map<int64_t, utils::chunked_vector<mutation>>& _buckets;

    std::optional<int64_t> _bucket_id;
    mutation_opt _current_mutation;
    bool _is_first_mutation = true;

    range_tombstone_change _current_rtc;

    throw_point* _throw_point = nullptr;

private:
    void check_bucket_id(int64_t bucket_id) {
        if (_bucket_id) {
            BOOST_REQUIRE_EQUAL(bucket_id, *_bucket_id);
        } else {
            _bucket_id = bucket_id;
        }
    }

    void check_timestamp(api::timestamp_type ts) {
        if (!std::holds_alternative<classify_by_timestamp>(_classify)) {
            return;
        }
        check_bucket_id(std::get<classify_by_timestamp>(_classify)(ts));
    }
    void check_token(dht::token t) {
        if (!std::holds_alternative<classify_by_token_group>(_classify)) {
            return;
        }
        check_bucket_id(std::get<classify_by_token_group>(_classify)(t));
    }

    void verify_column_bucket_id(const atomic_cell_or_collection& cell, const column_definition& cdef) {
        if (cdef.is_atomic()) {
            check_timestamp(cell.as_atomic_cell(cdef).timestamp());
        } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
            for (const auto& [key, c] : cell.as_collection_mutation()) {
                check_timestamp(c.timestamp());
            }
        } else {
            BOOST_FAIL(fmt::format("Failed to verify column bucket id: column {} is of unknown type {}", cdef.name_as_text(), cdef.type->name()));
        }
    }
    void verify_row_bucket_id(const row& r, column_kind kind) {
        r.for_each_cell([this, kind] (column_id id, const atomic_cell_or_collection& cell) {
            verify_column_bucket_id(cell, _schema->column_at(kind, id));
        });
    }
    void verify_partition_tombstone(tombstone tomb) {
        if (tomb) {
            check_timestamp(tomb.timestamp);
        }
    }
    void verify_static_row(const static_row& sr) {
        verify_row_bucket_id(sr.cells(), column_kind::static_column);
    }
    void verify_clustering_row(const clustering_row& cr) {
        if (!cr.marker().is_missing()) {
            check_timestamp(cr.marker().timestamp());
        }
        if (cr.tomb()) {
            check_timestamp(cr.tomb().tomb().timestamp);
        }
        verify_row_bucket_id(cr.cells(), column_kind::regular_column);
    }
    void verify_range_tombstone_change(const range_tombstone_change& rtc) {
        if (rtc.tombstone()) {
            check_timestamp(rtc.tombstone().timestamp);
        }
    }

    void maybe_throw(std::source_location sl = std::source_location::current()) {
        if (_throw_point) {
            _throw_point->check_throw(sl);
        }
    }

public:
    test_bucket_writer(schema_ptr schema, reader_permit permit, classify_by_var_t classify, std::unordered_map<int64_t,
            utils::chunked_vector<mutation>>& buckets, throw_point* tp = nullptr)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _classify(std::move(classify))
        , _buckets(buckets)
        , _current_rtc(position_in_partition::before_all_clustered_rows(), tombstone())
        , _throw_point(tp)
    { }
    void consume_new_partition(const dht::decorated_key& dk) {
        maybe_throw();
        BOOST_REQUIRE(!_current_mutation);
        _current_mutation = mutation(_schema, dk);
        check_token(dk.token());
    }
    void consume(tombstone partition_tombstone) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_partition_tombstone(partition_tombstone);
        _current_mutation->partition().apply(partition_tombstone);
    }
    stop_iteration consume(static_row&& sr) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_static_row(sr);
        _current_mutation->apply(mutation_fragment(*_schema, _permit, std::move(sr)));
        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_clustering_row(cr);
        _current_mutation->apply(mutation_fragment(*_schema, _permit, std::move(cr)));
        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone_change&& rtc) {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        verify_range_tombstone_change(rtc);
        if (_current_rtc.tombstone()) {
            auto rt = range_tombstone(_current_rtc.position(), position_in_partition_view::before_key(rtc.position()), _current_rtc.tombstone());
            _current_mutation->apply(mutation_fragment(*_schema, _permit, std::move(rt)));
        }
        _current_rtc = std::move(rtc);
        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        maybe_throw();
        BOOST_REQUIRE(_current_mutation);
        BOOST_REQUIRE(_bucket_id);
        BOOST_REQUIRE(!_current_rtc.tombstone());
        auto& bucket = _buckets[*_bucket_id];

        if (_is_first_mutation) {
            BOOST_REQUIRE(bucket.empty());
            _is_first_mutation = false;
        }

        bucket.emplace_back(std::move(*_current_mutation));
        _current_mutation = std::nullopt;
        return stop_iteration::no;
    }
    void consume_end_of_stream() {
        maybe_throw();
        BOOST_REQUIRE(!_current_mutation);
    }
};

} // anonymous namespace

using bucket_map_t = std::unordered_map<int64_t, utils::chunked_vector<mutation>>;

// requires seastar thread.
static void assert_that_segregator_produces_correct_data(const bucket_map_t& buckets, utils::chunked_vector<mutation>& muts, reader_permit permit, tests::random_schema& random_schema);

SEASTAR_THREAD_TEST_CASE(test_timestamp_based_splitting_mutation_writer) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    testlog.info("Random schema:\n{}", random_schema.cql());

    auto ts_gen = [&, underlying = tests::default_timestamp_generator()] (std::mt19937& engine,
            tests::timestamp_destination ts_dest, api::timestamp_type min_timestamp) -> api::timestamp_type {
        if (ts_dest == tests::timestamp_destination::partition_tombstone ||
                ts_dest == tests::timestamp_destination::row_marker ||
                ts_dest == tests::timestamp_destination::row_tombstone ||
                ts_dest == tests::timestamp_destination::collection_tombstone) {
            if (tests::random::get_int<int>(0, 10, engine)) {
                return api::missing_timestamp;
            }
        }
        return underlying(engine, ts_dest, min_timestamp);
    };

    auto muts = tests::generate_random_mutations(random_schema, ts_gen).get();

    auto classify_fn = [] (api::timestamp_type ts) {
        return int64_t(ts % 2);
    };

    std::unordered_map<int64_t, utils::chunked_vector<mutation>> buckets;

    auto consumer = [&] (mutation_reader bucket_reader) {
        return with_closeable(std::move(bucket_reader), [&] (mutation_reader& rd) {
            return rd.consume(test_bucket_writer(random_schema.schema(), rd.permit(), classify_fn, buckets));
        });
    };

    segregate_by_timestamp(make_mutation_reader_from_mutations(random_schema.schema(), semaphore.make_permit(), muts), classify_fn, std::move(consumer)).get();

    testlog.debug("Data split into {} buckets: {}", buckets.size(), buckets | std::views::keys | std::ranges::to<std::vector>());

    assert_that_segregator_produces_correct_data(buckets, muts, semaphore.make_permit(), random_schema);
}

static void assert_that_segregator_produces_correct_data(const bucket_map_t& buckets, utils::chunked_vector<mutation>& muts, reader_permit permit, tests::random_schema& random_schema) {
    auto bucket_readers = buckets | std::views::values |
            std::views::transform([&random_schema, &permit] (utils::chunked_vector<mutation> muts) { return make_mutation_reader_from_mutations(random_schema.schema(), permit, std::move(muts)); }) |
            std::ranges::to<std::vector>();
    auto reader = make_combined_reader(random_schema.schema(), permit, std::move(bucket_readers), streamed_mutation::forwarding::no,
            mutation_reader::forwarding::no);
    auto close_reader = deferred_close(reader);

    const auto now = gc_clock::now();
    for (auto& m : muts) {
        m.partition().compact_for_compaction(*random_schema.schema(), always_gc, m.decorated_key(), now, tombstone_gc_state::for_tests());
    }

    utils::chunked_vector<mutation> combined_mutations;
    while (auto m = read_mutation_from_mutation_reader(reader).get()) {
        m->partition().compact_for_compaction(*random_schema.schema(), always_gc, m->decorated_key(), now, tombstone_gc_state::for_tests());
        combined_mutations.emplace_back(std::move(*m));
    }

    BOOST_REQUIRE_EQUAL(combined_mutations.size(), muts.size());
    for (size_t i = 0; i < muts.size(); ++i) {
        testlog.debug("Comparing mutation #{}", i);
        assert_that(combined_mutations[i]).is_equal_to(muts[i]);
    }
}

SEASTAR_THREAD_TEST_CASE(test_timestamp_based_splitting_mutation_writer_abort) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    testlog.info("Random schema:\n{}", random_schema.cql());

    auto ts_gen = [&, underlying = tests::default_timestamp_generator()] (std::mt19937& engine,
            tests::timestamp_destination ts_dest, api::timestamp_type min_timestamp) -> api::timestamp_type {
        if (ts_dest == tests::timestamp_destination::partition_tombstone ||
                ts_dest == tests::timestamp_destination::row_marker ||
                ts_dest == tests::timestamp_destination::row_tombstone ||
                ts_dest == tests::timestamp_destination::collection_tombstone) {
            if (tests::random::get_int<int>(0, 10, engine)) {
                return api::missing_timestamp;
            }
        }
        return underlying(engine, ts_dest, min_timestamp);
    };

    auto muts = tests::generate_random_mutations(random_schema, ts_gen).get();

    auto classify_fn = [] (api::timestamp_type ts) {
        return int64_t(ts % 2);
    };

    std::unordered_map<int64_t, utils::chunked_vector<mutation>> buckets;

    const int throw_after = tests::random::get_int(muts.size() - 1);
    test_bucket_writer::throw_point tp(throw_after);
    testlog.info("Will raise exception after {}/{} mutations", throw_after, muts.size());
    auto consumer = [&] (mutation_reader bucket_reader) {
        return with_closeable(std::move(bucket_reader), [&] (mutation_reader& rd) {
            return rd.consume(test_bucket_writer(random_schema.schema(), rd.permit(), classify_fn, buckets, &tp));
        });
    };

    try {
        segregate_by_timestamp(make_mutation_reader_from_mutations(random_schema.schema(), semaphore.make_permit(), muts), classify_fn, std::move(consumer)).get();
    } catch (const test_bucket_writer::expected_exception&) {
        BOOST_TEST_PASSPOINT();
    } catch (const seastar::broken_promise&) {
        // Tolerated until we properly abort readers
        BOOST_TEST_PASSPOINT();
    }
}

// The tests below examine how the timestamp-based splitting writer treats a
// single row whose row_marker, cells and row tombstone carry different write
// timestamps. Time-window compaction uses this writer to segregate data into
// time windows, see time_window_compaction_strategy::make_interposer_consumer().
//
// This matters for materialized views, whose rows routinely have a marker
// timestamp that differs from the timestamps of their cells: the view row
// marker is derived from the base row's marker (or from the timestamp of the
// base column promoted into the view key), while the view cells keep the
// timestamps of the base cells they were generated from.

namespace {

// Stands in for a TWCS time window: a window covers 1000 timestamp units.
constexpr api::timestamp_type ts_window_size = 1000;

int64_t window_of(api::timestamp_type ts) {
    return ts / ts_window_size;
}

schema_ptr make_timestamp_split_schema() {
    return schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .build();
}

mutation make_single_row_mutation(schema_ptr s) {
    return mutation(s, partition_key::from_single_value(*s, int32_type->decompose(0)));
}

deletable_row& single_row(mutation& m) {
    auto& s = *m.schema();
    return m.partition().clustered_row(s, clustering_key::from_single_value(s, int32_type->decompose(0)));
}

const deletable_row& single_row(const mutation& m) {
    BOOST_REQUIRE_EQUAL(m.partition().clustered_rows().calculate_size(), 1);
    return m.partition().clustered_rows().begin()->row();
}

void set_cell(deletable_row& row, const schema& s, api::timestamp_type ts, int32_t value) {
    row.cells().apply(*s.get_column_definition("v"), atomic_cell::make_live(*int32_type, ts, int32_type->decompose(value)));
}

std::optional<api::timestamp_type> cell_timestamp(const deletable_row& row, const schema& s) {
    auto* cell = row.cells().find_cell(s.get_column_definition("v")->id);
    if (!cell) {
        return std::nullopt;
    }
    return cell->as_atomic_cell(*s.get_column_definition("v")).timestamp();
}

// Segregates `m` into windows with window_of() as the classifier and returns
// the mutation each bucket (i.e. each would-be sstable) received, ordered by
// window so that the result is deterministic.
// requires seastar thread.
utils::chunked_vector<mutation> segregate_into_windows(reader_permit permit, const mutation& m) {
    auto s = m.schema();
    utils::chunked_vector<mutation> bucket_mutations;
    auto consumer = [&] (mutation_reader bucket_reader) -> future<> {
        auto close = deferred_close(bucket_reader);
        while (auto bm = co_await read_mutation_from_mutation_reader(bucket_reader)) {
            bucket_mutations.emplace_back(std::move(*bm));
        }
    };
    segregate_by_timestamp(make_mutation_reader_from_mutations(s, permit, {m}), window_of, std::move(consumer)).get();
    return bucket_mutations;
}

// All timestamps a bucket's row carries, so tests can assert that a bucket is
// timestamp-homogeneous, i.e. that it really does belong to a single window.
std::set<api::timestamp_type> row_timestamps(const mutation& m) {
    const auto& row = single_row(m);
    std::set<api::timestamp_type> timestamps;
    if (!row.marker().is_missing()) {
        timestamps.insert(row.marker().timestamp());
    }
    if (row.deleted_at().regular()) {
        timestamps.insert(row.deleted_at().regular().timestamp);
    }
    if (row.deleted_at()) {
        timestamps.insert(row.deleted_at().tomb().timestamp);
    }
    if (auto ts = cell_timestamp(row, *m.schema())) {
        timestamps.insert(*ts);
    }
    return timestamps;
}

// Merges the per-window mutations back and checks that nothing was lost or
// altered by the split, which is what the read path relies on.
void verify_recombines_to(const utils::chunked_vector<mutation>& bucket_mutations, const mutation& expected) {
    auto combined = mutation(expected.schema(), expected.decorated_key());
    for (const auto& bm : bucket_mutations) {
        combined.apply(bm);
    }
    assert_that(combined).is_equal_to(expected);
}

} // anonymous namespace

// A row whose marker and cells were written at timestamps belonging to
// different windows is split: the marker lands in one window and the cells in
// another, so the row exists in two sstables that TWCS will never compact
// together.
SEASTAR_THREAD_TEST_CASE(test_timestamp_splitting_separates_row_marker_from_cells) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_timestamp_split_schema();

    const api::timestamp_type marker_ts = 1000;
    const api::timestamp_type cell_ts = 2000;
    BOOST_REQUIRE_NE(window_of(marker_ts), window_of(cell_ts));

    auto m = make_single_row_mutation(s);
    auto& row = single_row(m);
    row.apply(row_marker(marker_ts));
    set_cell(row, *s, cell_ts, 1);

    auto bucket_mutations = segregate_into_windows(semaphore.make_permit(), m);

    // The single row was split in two.
    BOOST_REQUIRE_EQUAL(bucket_mutations.size(), 2);
    for (const auto& bm : bucket_mutations) {
        BOOST_REQUIRE_EQUAL(row_timestamps(bm).size(), 1);
    }

    const auto marker_bucket = std::ranges::find_if(bucket_mutations, [] (const mutation& bm) {
        return !single_row(bm).marker().is_missing();
    });
    BOOST_REQUIRE(marker_bucket != bucket_mutations.end());
    BOOST_REQUIRE_EQUAL(single_row(*marker_bucket).marker().timestamp(), marker_ts);
    // The marker's window carries no cells...
    BOOST_REQUIRE(!cell_timestamp(single_row(*marker_bucket), *s));

    const auto cell_bucket = std::ranges::find_if(bucket_mutations, [&] (const mutation& bm) {
        return &bm != &*marker_bucket;
    });
    // ... and the cells' window carries no marker, so on its own it looks like
    // a row that was never inserted.
    BOOST_REQUIRE(single_row(*cell_bucket).marker().is_missing());
    BOOST_REQUIRE_EQUAL(cell_timestamp(single_row(*cell_bucket), *s).value(), cell_ts);

    verify_recombines_to(bucket_mutations, m);
}

// A shadowable tombstone is segregated by its own timestamp, away from the
// cells it shadows. The window holding those cells therefore has no tombstone
// covering them, and since TWCS compacts each window on its own, it can never
// reclaim them.
SEASTAR_THREAD_TEST_CASE(test_timestamp_splitting_separates_shadowable_tombstone_from_shadowed_cells) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_timestamp_split_schema();

    const api::timestamp_type cell_ts = 1000;
    const api::timestamp_type deletion_ts = 2000;
    BOOST_REQUIRE_NE(window_of(cell_ts), window_of(deletion_ts));

    // The state of a view row whose base row was updated such that it no longer
    // maps to this view key: the old cells are still there and a shadowable
    // tombstone deletes them.
    auto m = make_single_row_mutation(s);
    auto& row = single_row(m);
    set_cell(row, *s, cell_ts, 1);
    row.apply(shadowable_tombstone(deletion_ts, gc_clock::now()));
    BOOST_REQUIRE(bool(row.deleted_at().is_shadowable()));

    auto bucket_mutations = segregate_into_windows(semaphore.make_permit(), m);

    BOOST_REQUIRE_EQUAL(bucket_mutations.size(), 2);

    const auto tomb_bucket = std::ranges::find_if(bucket_mutations, [] (const mutation& bm) {
        return bool(single_row(bm).deleted_at());
    });
    BOOST_REQUIRE(tomb_bucket != bucket_mutations.end());
    // The tombstone keeps its shadowable flag across the split.
    BOOST_REQUIRE(bool(single_row(*tomb_bucket).deleted_at().is_shadowable()));
    BOOST_REQUIRE_EQUAL(single_row(*tomb_bucket).deleted_at().tomb().timestamp, deletion_ts);
    BOOST_REQUIRE(!cell_timestamp(single_row(*tomb_bucket), *s));

    const auto cell_bucket = std::ranges::find_if(bucket_mutations, [&] (const mutation& bm) {
        return &bm != &*tomb_bucket;
    });
    BOOST_REQUIRE_EQUAL(cell_timestamp(single_row(*cell_bucket), *s).value(), cell_ts);
    BOOST_REQUIRE(!single_row(*cell_bucket).deleted_at());

    // Compacting the cells' window on its own -- what TWCS does, since it never
    // mixes windows -- does not drop the shadowed cells, even with everything
    // gc-able: the tombstone that covers them is in another window.
    auto cells_only = *cell_bucket;
    cells_only.partition().compact_for_compaction(*s, always_gc, cells_only.decorated_key(), gc_clock::now(),
            tombstone_gc_state::for_tests());
    BOOST_REQUIRE_EQUAL(cell_timestamp(single_row(cells_only), *s).value(), cell_ts);

    // Reads are unaffected: merging the windows back reproduces the row.
    verify_recombines_to(bucket_mutations, m);
}

// row_tombstone is classified by row_tombstone::tomb(), which is the greater of
// the regular and the shadowable tombstone. A regular tombstone paired with a
// newer shadowable one is therefore filed under the shadowable tombstone's
// window, not its own.
SEASTAR_THREAD_TEST_CASE(test_timestamp_splitting_classifies_row_tombstone_by_shadowable_timestamp) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto s = make_timestamp_split_schema();

    const api::timestamp_type regular_ts = 1000;
    const api::timestamp_type shadowable_ts = 2000;
    BOOST_REQUIRE_NE(window_of(regular_ts), window_of(shadowable_ts));

    auto m = make_single_row_mutation(s);
    auto& row = single_row(m);
    row.apply(tombstone(regular_ts, gc_clock::now()));
    row.apply(shadowable_tombstone(shadowable_ts, gc_clock::now()));
    BOOST_REQUIRE_EQUAL(row.deleted_at().regular().timestamp, regular_ts);
    BOOST_REQUIRE_EQUAL(row.deleted_at().shadowable().tomb().timestamp, shadowable_ts);

    auto bucket_mutations = segregate_into_windows(semaphore.make_permit(), m);

    // Both tombstones are written to a single bucket ...
    BOOST_REQUIRE_EQUAL(bucket_mutations.size(), 1);
    const auto& tomb = single_row(bucket_mutations.front()).deleted_at();
    BOOST_REQUIRE_EQUAL(tomb.regular().timestamp, regular_ts);
    BOOST_REQUIRE_EQUAL(tomb.shadowable().tomb().timestamp, shadowable_ts);
    // ... the one of the shadowable tombstone, so the regular tombstone ends up
    // in a window its own timestamp does not belong to.
    BOOST_REQUIRE_EQUAL(row_timestamps(bucket_mutations.front()).size(), 2);

    verify_recombines_to(bucket_mutations, m);
}

// Check that the partition_based_splitting_mutation_writer can fix reordered partitions
SEASTAR_THREAD_TEST_CASE(test_partition_based_splitting_mutation_writer) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 2),
            std::uniform_int_distribution<size_t>(0, 2),
            std::uniform_int_distribution<size_t>(1, 2),
            std::uniform_int_distribution<size_t>(0, 1));

    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    const auto input_mutations = tests::generate_random_mutations(
            random_schema,
            tests::default_timestamp_generator(),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(100, 1000), // partitions
            std::uniform_int_distribution<size_t>(1, 4), // rows
            std::uniform_int_distribution<size_t>(0, 1)).get(); // range tombstones

    auto shuffled_input_mutations = input_mutations;
    shuffled_input_mutations.emplace_back(*shuffled_input_mutations.begin()); // Have a duplicate partition as well.
    std::shuffle(shuffled_input_mutations.begin(), shuffled_input_mutations.end(), tests::random::gen());

    testlog.info("input_mutations.size()={}", input_mutations.size());

    std::vector<utils::chunked_vector<mutation>> output_mutations;
    size_t next_index = 0;

    auto consumer = [&] (mutation_reader rd) {
        const auto index = next_index++;
        output_mutations.emplace_back();
        BOOST_REQUIRE_EQUAL(output_mutations.size(), next_index);

        return async([&, index, rd = std::move(rd)] () mutable {
            auto close_rd = deferred_close(rd);
            mutation_rebuilder_v2 mut_builder(rd.schema());
            mutation_fragment_stream_validating_filter validator("test", *rd.schema(), mutation_fragment_stream_validation_level::clustering_key);
            while (auto mf_opt = rd().get()) {
                if (mf_opt->is_partition_start()) {
                    const auto& key = mf_opt->as_partition_start().key();
                    validator(key);
                }
                validator(*mf_opt);
                if (mf_opt->is_end_of_partition()) {
                    output_mutations[index].emplace_back(*std::move(mut_builder).consume_end_of_stream());
                } else {
                    std::move(*mf_opt).consume(mut_builder);
                }
            }
            validator.on_end_of_stream();
        });
    };
    auto check_and_reset = [&] {
        std::vector<mutation_reader> readers;
        auto close_readers = defer([&] noexcept {
            for (auto& rd : readers) {
                rd.close().get();
            }
        });
        for (auto muts : output_mutations) {
            readers.emplace_back(make_mutation_reader_from_mutations(random_schema.schema(), semaphore.make_permit(), std::move(muts)));
        }
        auto rd = assert_that(make_combined_reader(random_schema.schema(), semaphore.make_permit(), std::move(readers)));
        for (const auto& mut : input_mutations) {
            rd.produces(mut);
        }
        output_mutations.clear();
        next_index = 0;
    };

    for (const size_t max_memory : {1'000, 10'000, 1'000'000, 10'000'000, 100'000'000}) {
        testlog.info("Segregating with in-memory method (max_memory={})", max_memory);
        mutation_writer::segregate_by_partition(
                make_mutation_reader_from_mutations(random_schema.schema(), semaphore.make_permit(), shuffled_input_mutations),
                mutation_writer::segregate_config{max_memory},
                consumer).get();
        testlog.info("Done segregating with in-memory method (max_memory={}): input segregated into {} buckets", max_memory, output_mutations.size());
        check_and_reset();
    }

}

SEASTAR_THREAD_TEST_CASE(test_token_group_based_splitting_mutation_writer) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    testlog.info("Random schema:\n{}", random_schema.cql());

    size_t partition_count = many_partitions();

    auto muts = tests::generate_random_mutations(
            random_schema,
            tests::default_timestamp_generator(),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(partition_count, partition_count),
            std::uniform_int_distribution<size_t>(1, 1)).get(); // only 1 row

    auto classify_fn = [] (dht::token t) -> mutation_writer::token_group_id {
        return dht::compaction_group_of(1, t);
    };

    std::unordered_map<int64_t, utils::chunked_vector<mutation>> buckets;

    auto consumer = [&] (mutation_reader bucket_reader) {
        return with_closeable(std::move(bucket_reader), [&] (mutation_reader& rd) {
            return rd.consume(test_bucket_writer(random_schema.schema(), rd.permit(), classify_fn, buckets));
        });
    };

    segregate_by_token_group(make_mutation_reader_from_mutations(random_schema.schema(), semaphore.make_permit(), muts), classify_fn, std::move(consumer)).get();

    testlog.info("Data split into {} buckets: {}", buckets.size(), buckets | std::views::keys | std::ranges::to<std::vector>());

    assert_that_segregator_produces_correct_data(buckets, muts, semaphore.make_permit(), random_schema);
}

// Reproducer for https://scylladb.atlassian.net/browse/SCYLLADB-2397
// A split compaction interrupted due to abort was exiting with "Dangling queue_reader_handle"
// instead of propagating the original abort exception cleanly.
// This test injects an exception at every possible consume call site and verifies that only
// the expected exception propagates out (never a dangling-handle error).
SEASTAR_THREAD_TEST_CASE(test_token_group_based_splitting_mutation_writer_abort) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto random_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
    auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};

    testlog.info("Random schema:\n{}", random_schema.cql());

    auto muts = tests::generate_random_mutations(
            random_schema,
            tests::default_timestamp_generator(),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(2, 3),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(1, 2)).get();

    testlog.info("Generated {} mutations", muts.size());

    auto classify_fn = [&] (dht::token t) -> mutation_writer::token_group_id {
        if (muts[0].token() == t) {
            return 0;
        }
        return 1;
    };

    // Mutations are split into two groups: left and right, see classify_fn above.
    // Each has its own consumer (writer), which need their own throw point.
    test_bucket_writer::throw_point tp_left(0);
    test_bucket_writer::throw_point tp_right(0);
    do {
        std::unordered_map<int64_t, utils::chunked_vector<mutation>> buckets;

        auto consumer = [&, i = 0] (mutation_reader reader) mutable {
            // Mimick compaction use-case, which uses seastar thread for writers
            return seastar::async([&, reader = std::move(reader)] () mutable {
                auto close_reader = deferred_close(reader);

                auto* tp = i == 0 ? &tp_left : &tp_right;
                testlog.debug("Creating consumer for {} with throw-point after {}", i == 0 ? "left" : "right", tp->throw_after());
                ++i;

                reader.consume(test_bucket_writer(random_schema.schema(), reader.permit(), classify_fn, buckets, tp)).get();
            });
        };

        try {
            segregate_by_token_group(
                make_mutation_reader_from_mutations(random_schema.schema(), semaphore.make_permit(), muts),
                classify_fn,
                std::move(consumer)).get();
        } catch (const test_bucket_writer::expected_exception&) {
            BOOST_TEST_PASSPOINT();
        } catch (...) {
            BOOST_FAIL(fmt::format("Unexpected exception at throw_point_left={}, throw_point_right={}: {}", tp_left.throw_after(), tp_right.throw_after(), std::current_exception()));
        }
    } while (tp_left.check_hit_and_advance() || tp_right.check_hit_and_advance());
}

BOOST_AUTO_TEST_SUITE_END()
