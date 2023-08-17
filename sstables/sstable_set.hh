/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "readers/flat_mutation_reader_fwd.hh"
#include "readers/flat_mutation_reader_v2.hh"
#include "sstables/progress_monitor.hh"
#include "shared_sstable.hh"
#include "dht/i_partitioner.hh"
#include <seastar/core/shared_ptr.hh>
#include <type_traits>
#include <vector>

namespace utils {
class estimated_histogram;
}

namespace sstables {

struct sstable_first_key_less_comparator {
    bool operator()(const shared_sstable& s1, const shared_sstable& s2) const;
};

// Structure holds all sstables (a.k.a. fragments) that belong to same run identifier, which is an UUID.
// SStables in that same run will not overlap with one another.
class sstable_run {
public:
    using sstable_set = std::set<shared_sstable, sstable_first_key_less_comparator>;
private:
    sstable_set _all;
private:
    bool will_introduce_overlapping(const shared_sstable& sst) const;
public:
    // Returns false if sstable being inserted cannot satisfy the disjoint invariant. Then caller should pick another run for it.
    bool insert(shared_sstable sst);
    void erase(shared_sstable sst);
    bool empty() const noexcept {
        return _all.empty();
    }
    // Data size of the whole run, meaning it's a sum of the data size of all its fragments.
    uint64_t data_size() const;
    const sstable_set& all() const { return _all; }
    double estimate_droppable_tombstone_ratio(gc_clock::time_point gc_before) const;
};

class sstable_set_impl;
using sstable_set_impl_ptr = seastar::shared_ptr<sstable_set_impl>;

class incremental_selector_impl {
    sstable_set_impl_ptr _set_impl;
public:
    incremental_selector_impl() = delete;
    explicit incremental_selector_impl(sstable_set_impl_ptr set_impl) noexcept : _set_impl(std::move(set_impl)) {}
    virtual ~incremental_selector_impl() {}
    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const dht::ring_position_view&) = 0;
};

using sstable_predicate = noncopyable_function<bool(const sstable&)>;
// Default predicate includes everything
const sstable_predicate& default_sstable_predicate();

class sstable_set_impl {
protected:
    uint64_t _bytes_on_disk = 0;

    // for cloning
    explicit sstable_set_impl(uint64_t bytes_on_disk) noexcept : _bytes_on_disk(bytes_on_disk) {}
public:
    sstable_set_impl() = default;
    sstable_set_impl(const sstable_set_impl&) = default;
    virtual ~sstable_set_impl() {}
    virtual sstable_set_impl_ptr clone() const = 0;
    virtual std::vector<shared_sstable> select(const dht::partition_range& range) const = 0;
    virtual std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const;
    virtual lw_shared_ptr<const sstable_list> all() const = 0;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const = 0;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const = 0;
    // Return true iff sst was inserted
    virtual bool insert(shared_sstable sst) = 0;
    // Return true iff sst was erased
    virtual bool erase(shared_sstable sst) = 0;
    // Returns a vector holding the unique sstables that were inserted
    // Guarantees strong exception safety
    virtual std::vector<shared_sstable> insert(const std::span<shared_sstable>& sstables);
    virtual size_t size() const noexcept = 0;
    virtual uint64_t bytes_on_disk() const noexcept {
        return _bytes_on_disk;
    }
    uint64_t add_bytes_on_disk(uint64_t delta) noexcept {
        return _bytes_on_disk += delta;
    }
    uint64_t sub_bytes_on_disk(uint64_t delta) noexcept {
        return _bytes_on_disk -= delta;
    }
    virtual std::unique_ptr<incremental_selector_impl> make_incremental_selector(sstable_set_impl_ptr set_impl) const = 0;

    virtual flat_mutation_reader_v2 create_single_key_sstable_reader(
        replica::column_family*,
        schema_ptr,
        reader_permit,
        utils::estimated_histogram&,
        const dht::partition_range&,
        const query::partition_slice&,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        const sstable_predicate&) const;
};

class sstable_set {
    sstable_set_impl_ptr _impl;
    schema_ptr _schema;
public:
    sstable_set() = default;
    ~sstable_set();
    sstable_set(sstable_set_impl_ptr impl, schema_ptr s);
    // Creates a copy sharing the impl_ptr
    sstable_set(const sstable_set&) = default;
    sstable_set(sstable_set&&) noexcept = default;
    // Creates a copy sharing the impl_ptr
    sstable_set& operator=(const sstable_set&);
    sstable_set& operator=(sstable_set&&) noexcept;

    explicit operator bool() const noexcept {
        return _impl != nullptr;
    }

    // Clones a copy of the impl_ptr
    // Can be used for preparing an updated copy
    // that can be move assigned to the origin sstable_set for in-place update.
    // This is useful for updating a sstable_set that is contained in a compound_sstable_set
    sstable_set clone() const;
    std::vector<shared_sstable> select(const dht::partition_range& range) const;
    // Return all runs which contain any of the input sstables.
    std::vector<sstable_run> select_sstable_runs(const std::vector<shared_sstable>& sstables) const;
    // Return all sstables. It's not guaranteed that sstable_set will keep a reference to the returned list, so user should keep it.
    lw_shared_ptr<const sstable_list> all() const;
    // Prefer for_each_sstable() over all() for iteration purposes, as the latter may have to copy all sstables into a temporary
    void for_each_sstable(std::function<void(const shared_sstable&)> func) const;
    template <typename Func>
    requires std::same_as<typename futurize<std::invoke_result_t<Func, shared_sstable>>::type, future<>>
    future<> for_each_sstable_gently(Func&& func) const {
        using futurator = futurize<std::invoke_result_t<Func, shared_sstable>>;
        auto impl = _impl;
        co_await impl->for_each_sstable_gently_until([func = std::forward<Func>(func)] (const shared_sstable& sst) -> future<stop_iteration> {
            co_await futurator::invoke(func, sst);
            co_return stop_iteration::no;
        });
    }
    // Calls func for each sstable or until it returns stop_iteration::yes
    // Returns the last stop_iteration value.
    stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const;
    template <typename Func>
    requires std::same_as<typename futurize<std::invoke_result_t<Func, shared_sstable>>::type, future<stop_iteration>>
    future<stop_iteration> for_each_sstable_gently_until(Func&& func) const {
        auto impl = _impl;
        co_return co_await impl->for_each_sstable_gently_until([func = std::forward<Func>(func)] (const shared_sstable& sst) -> future<stop_iteration> {
            using futurator = futurize<std::invoke_result_t<Func, shared_sstable>>;
            return futurator::invoke(func, sst);
        });
    }
    // Return true iff sst was inserted
    bool insert(shared_sstable sst);
    // Return true iff sst was erase
    bool erase(shared_sstable sst);
    // Returns a vector holding the unique sstables that were inserted
    // Guarantees strong exception safety
    std::vector<shared_sstable> insert(const std::span<shared_sstable>& sstables);
    size_t size() const noexcept;
    uint64_t bytes_on_disk() const noexcept;

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
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        const sstable_predicate& p = default_sstable_predicate()) const;

    /// Read a range from the sstable set.
    ///
    /// The reader is unrestricted, but will account its resource usage on the
    /// semaphore belonging to the passed-in permit.
    flat_mutation_reader_v2 make_range_sstable_reader(
        schema_ptr,
        reader_permit,
        const dht::partition_range&,
        const query::partition_slice&,
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
        tracing::trace_state_ptr,
        streamed_mutation::forwarding,
        mutation_reader::forwarding,
        read_monitor_generator& rmg = default_read_monitor_generator(),
        const sstable_predicate& p = default_sstable_predicate()) const;

    flat_mutation_reader_v2 make_crawling_reader(
            schema_ptr,
            reader_permit,
            tracing::trace_state_ptr,
            read_monitor_generator& rmg = default_read_monitor_generator()) const;

    friend class compound_sstable_set;
};

sstable_set make_partitioned_sstable_set(schema_ptr schema, bool use_level_metadata = true);

sstable_set make_compound_sstable_set(schema_ptr schema, std::vector<sstable_set> sets);

std::ostream& operator<<(std::ostream& os, const sstables::sstable_run& run);

using offstrategy = bool_class<class offstrategy_tag>;

/// Return the amount of overlapping in a set of sstables. 0 is returned if set is disjoint.
///
/// The 'sstables' parameter must be a set of sstables sorted by first key.
unsigned sstable_set_overlapping_count(const schema_ptr& schema, const std::vector<shared_sstable>& sstables);

}
