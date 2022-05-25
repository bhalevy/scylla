/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/coroutine.hh>

#include "readers/flat_mutation_reader.hh"
#include "readers/flat_mutation_reader_v2.hh"
#include "sstables/progress_monitor.hh"
#include "shared_sstable.hh"
#include "dht/i_partitioner.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/io_priority_class.hh>
#include <vector>

namespace utils {
class estimated_histogram;
}

namespace sstables {

class sstable_set_impl;
class incremental_selector_impl;

class sstable_map : public std::unordered_map<generation_type, shared_sstable>, public enable_lw_shared_from_this<sstable_map> {
    using map_type = std::unordered_map<generation_type, shared_sstable>;
public:
    sstable_map() = default;

    explicit sstable_map(shared_sstable);
    sstable_map(const std::vector<shared_sstable>&);
    sstable_map(std::vector<shared_sstable>&&);

    std::vector<shared_sstable> sstables() const;

    bool contains(generation_type gen) const noexcept {
        return map_type::contains(gen);
    }

    // Throws std::runtime_error is generation exists in map
    // but belongs to a different shared_sstable
    bool contains(const shared_sstable&) const;

    std::pair<iterator, bool> emplace(shared_sstable);

    // Throws std::runtime_error is generation exists in map
    // but belongs to a different shared_sstable
    std::pair<iterator, bool> insert(shared_sstable);

    size_type erase(generation_type gen) {
        return map_type::erase(gen);
    }

    // Throws std::runtime_error is generation exists in map
    // but belongs to a different shared_sstable
    size_type erase(shared_sstable);

    void for_each_sstable(std::function<void(const shared_sstable&)> func) const {
        for (const auto& [gen, sst] : *this) {
            func(sst);
        }
    }

    template <typename Func>
    requires std::same_as<futurize_t<std::invoke_result_t<Func, const shared_sstable&>>, future<>>
    future<> for_each_sstable_gently(Func func) const {
        auto zis = shared_from_this();
        for (auto& [gen, sst] : *this) {
            co_await futurize_invoke(func, sst);
        }
    }

    template <typename T, typename Func>
    requires std::same_as<futurize_t<std::invoke_result_t<Func, T, const shared_sstable&>>, future<T>>
    future<T> accumulate(T value, Func func) const {
        auto zis = shared_from_this();
        for (auto& [gen, sst] : *this) {
            value = co_await futurize_invoke(func, std::move(value), sst);
        }
        co_return value;
    }
};

// Structure holds all sstables (a.k.a. fragments) that belong to same run identifier, which is an UUID.
// SStables in that same run will not overlap with one another.
class sstable_run {
    std::unordered_set<shared_sstable> _all;
public:
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);
    // Data size of the whole run, meaning it's a sum of the data size of all its fragments.
    uint64_t data_size() const;
    const std::unordered_set<shared_sstable>& all() const { return _all; }
    double estimate_droppable_tombstone_ratio(gc_clock::time_point gc_before) const;
};

class sstable_set : public enable_lw_shared_from_this<sstable_set> {
    std::unique_ptr<sstable_set_impl> _impl;
    schema_ptr _schema;
public:
    ~sstable_set();
    sstable_set(std::unique_ptr<sstable_set_impl> impl, schema_ptr s);
    sstable_set(const sstable_set&);
    sstable_set(sstable_set&&) noexcept;
    sstable_set& operator=(const sstable_set&);
    sstable_set& operator=(sstable_set&&) noexcept;
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    // Return all runs which contain any of the input sstables.
    std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const;
    // Return all sstables. It's not guaranteed that sstable_set will keep a reference to the returned list, so user should keep it.
    lw_shared_ptr<sstable_map> all() const;
    // Prefer for_each_sstable() over all() for iteration purposes, as the latter may have to copy all sstables into a temporary
    void for_each_sstable(std::function<void(const shared_sstable&)> func) const;
    bool contains(generation_type gen) const noexcept;
    void insert(shared_sstable sst);
    void erase(shared_sstable sst);

    // Used to incrementally select sstables from sstable set using ring-position.
    // sstable set must be alive during the lifetime of the selector.
    class incremental_selector {
        std::unique_ptr<incremental_selector_impl> _impl;
        dht::ring_position_comparator _cmp;
        mutable std::optional<dht::partition_range> _current_range;
        mutable std::optional<nonwrapping_range<dht::ring_position_view>> _current_range_view;
        mutable std::vector<shared_sstable> _current_sstables;
        mutable dht::ring_position_ext _current_next_position = dht::ring_position_view::min();
    public:
        ~incremental_selector();
        incremental_selector(std::unique_ptr<incremental_selector_impl> impl, const schema& s);
        incremental_selector(incremental_selector&&) noexcept;

        struct selection {
            const std::vector<shared_sstable>& sstables;
            dht::ring_position_view next_position;
        };

        // Return the sstables that intersect with `pos` and the next
        // position where the intersecting sstables change.
        // To walk through the token range incrementally call `select()`
        // with `dht::ring_position_view::min()` and then pass back the
        // returned `next_position` on each next call until
        // `next_position` becomes `dht::ring_position::max()`.
        //
        // Successive calls to `select()' have to pass weakly monotonic
        // positions (incrementability).
        //
        // NOTE: both `selection.sstables` and `selection.next_position`
        // are only guaranteed to be valid until the next call to
        // `select()`.
        selection select(const dht::ring_position_view& pos) const;
    };
    incremental_selector make_incremental_selector() const;

    flat_mutation_reader_v2 create_single_key_sstable_reader(
        replica::column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&, // must be singular and contain a key
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding) const;

    /// Read a range from the sstable set.
    ///
    /// The reader is unrestricted, but will account its resource usage on the
    /// semaphore belonging to the passed-in permit.
    flat_mutation_reader_v2 make_range_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator()) const;

    // Filters out mutations that don't belong to the current shard.
    flat_mutation_reader_v2 make_local_shard_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
        const io_priority_class&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator()) const;

    flat_mutation_reader_v2 make_crawling_reader(
            schema_ptr,
            reader_permit,
            const io_priority_class&,
            tracing::trace_state_ptr,
            read_monitor_generator& rmg = default_read_monitor_generator()) const;

    friend class compound_sstable_set;
};

sstable_set make_partitioned_sstable_set(schema_ptr schema, lw_shared_ptr<sstable_map> all, bool use_level_metadata = true);

sstable_set make_compound_sstable_set(schema_ptr schema, std::vector<lw_shared_ptr<sstable_set>> sets);

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run);

using offstrategy = bool_class<class offstrategy_tag>;

/// Return the amount of overlapping in a set of sstables. 0 is returned if set is disjoint.
///
/// The 'sstables' parameter must be a set of sstables sorted by first key.
unsigned sstable_set_overlapping_count(const schema_ptr& schema, const std::vector<shared_sstable>& sstables);

class formatted_sstables_list {
    bool _include_origin = true;
    std::vector<sstring> _ssts;
public:
    formatted_sstables_list() = default;
    explicit formatted_sstables_list(const std::vector<shared_sstable>& ssts, bool include_origin = true);
    formatted_sstables_list& operator+=(const shared_sstable& sst);
    const std::vector<sstring>& ssts() const noexcept { return _ssts; }
};

} // namspace sstables

namespace std {

std::ostream& operator<<(std::ostream& os, const sstables::formatted_sstables_list& lst);

} // namespace std
