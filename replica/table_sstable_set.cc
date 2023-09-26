/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "dht/token.hh"
#include "replica/database.hh"
#include "replica/compaction_group.hh"
#include "sstables/shared_sstable.hh"
#include "log.hh"

using namespace sstables;

namespace replica {

extern logging::logger tlogger;

// this sstable set incrementally consumes unserlying sstable sets
// the managed sets cannot be modified through table_sstable_set, but only jointly read from, so insert() and erase() are disabled.
class table_sstable_set : public sstable_set_impl {
    table& _table;

public:
    table_sstable_set(table& t) noexcept
        : _table(t)
    {}

    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<table_sstable_set>(_table);
    }

    static sstable_set make(table& t) {
        return sstable_set(std::make_unique<table_sstable_set>(t));
    }

    virtual std::vector<shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const override;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const override;
    virtual bool insert(shared_sstable sst) override;
    virtual bool erase(shared_sstable sst) override;
    virtual size_t size() const noexcept override;
    virtual uint64_t bytes_on_disk() const noexcept override;
    virtual selector_and_schema_t make_incremental_selector() const override;

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
            const sstable_predicate&) const override;

    class incremental_selector;
private:
    // The for_each_sstable_set_* helpers guarantee atomicity
    // only for each compaction group' compound_sstable_set,
    // but not across compaction groups.
    stop_iteration for_each_sstable_set_until(const dht::partition_range&, std::function<stop_iteration(lw_shared_ptr<sstable_set>)>) const;
    future<stop_iteration> for_each_sstable_set_gently_until(const dht::partition_range&, std::function<future<stop_iteration>(lw_shared_ptr<sstable_set>)>) const;
};

stop_iteration table_sstable_set::for_each_sstable_set_until(const dht::partition_range& pr, std::function<stop_iteration(lw_shared_ptr<sstable_set>)> func) const {
    if (auto scg = _table.single_compaction_group_if_available()) {
        return func(scg->make_compound_sstable_set());
    }
    return _table.foreach_compaction_group_until(pr, [func = std::move(func)] (compaction_group& cg) {
        return func(cg.make_compound_sstable_set());
    });
}

future<stop_iteration> table_sstable_set::for_each_sstable_set_gently_until(const dht::partition_range& pr, std::function<future<stop_iteration>(lw_shared_ptr<sstable_set>)> func) const {
    if (auto scg = _table.single_compaction_group_if_available()) {
        return func(scg->make_compound_sstable_set());
    }
    return _table.foreach_compaction_group_gently_until(pr, [func = std::move(func)] (compaction_group& cg) {
        return func(cg.make_compound_sstable_set());
    });
}

std::vector<shared_sstable> table_sstable_set::select(const dht::partition_range& range) const {
    std::vector<shared_sstable> ret;
    for_each_sstable_set_until(range, [&] (lw_shared_ptr<sstable_set> set) {
        auto ssts = set->select(range);
        if (ret.empty()) {
            ret = std::move(ssts);
        } else {
            if (ssts.size() > 1) {
                ret.reserve(ret.size() + ssts.size());
            }
            std::move(ssts.begin(), ssts.end(), std::back_inserter(ret));
        }
        return stop_iteration::no;
    });
    tlogger.info("table_sstable_set::select: range={} ret={}", range, ret.size());
    return ret;
}

lw_shared_ptr<const sstable_list> table_sstable_set::all() const {
    auto ret = make_lw_shared<sstable_list>();
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstable_set> set) {
        set->for_each_sstable([&] (const shared_sstable& sst) {
            ret->insert(sst);
        });
        return stop_iteration::no;
    });
    return ret;
}

stop_iteration table_sstable_set::for_each_sstable_until(std::function<stop_iteration(const shared_sstable&)> func) const {
    return for_each_sstable_set_until(query::full_partition_range, [func = std::move(func)] (lw_shared_ptr<sstable_set> set) {
        return set->for_each_sstable_until(func);
    });
}

future<stop_iteration> table_sstable_set::for_each_sstable_gently_until(std::function<future<stop_iteration>(const shared_sstable&)> func) const {
    return for_each_sstable_set_gently_until(query::full_partition_range, [func = std::move(func)] (lw_shared_ptr<sstable_set> set) {
        return set->for_each_sstable_gently_until(func);
    });
}

bool table_sstable_set::insert(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}
bool table_sstable_set::erase(shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}

size_t
table_sstable_set::size() const noexcept {
    size_t ret = 0;
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstable_set> set) {
        ret += set->size();
        return stop_iteration::no;
    });
    return ret;
}

uint64_t
table_sstable_set::bytes_on_disk() const noexcept {
    uint64_t ret = 0;
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<sstable_set> set) {
        ret += set->bytes_on_disk();
        return stop_iteration::no;
    });
    return ret;
}

class table_incremental_selector : public incremental_selector_impl {
    table& _table;

    // _cur_set and _cur_selector contain a snapshot
    // for the currently selected compaction_group.
    // _lowest_next_position corresponds the start position
    // of the compaction_group following the currently selected one
    lw_shared_ptr<sstable_set> _cur_set;
    std::optional<sstable_set::incremental_selector> _cur_selector;
    dht::ring_position_ext _lowest_next_position = dht::ring_position_ext::max();

public:
    table_incremental_selector(table& t)
            : _table(t)
    {
        if (auto cg = _table.single_compaction_group_if_available()) {
            _cur_set = cg->make_compound_sstable_set();
            _cur_selector.emplace(_cur_set->make_incremental_selector());
        }
    }

    virtual std::tuple<dht::partition_range, std::vector<shared_sstable>, dht::ring_position_ext> select(const dht::ring_position_view& pos) override {
        // Always return minimum singular range, such that incremental_selector::select() will always call this function,
        // which in turn will find the next sstable set to select sstables from.
        const dht::partition_range current_range = dht::partition_range::make_singular(dht::ring_position::min());

        if (!_cur_selector) {
            auto* sg = _table.storage_group_for_token(pos.token());
            _cur_set = sg->make_compound_sstable_set();
            _cur_selector.emplace(_cur_set->make_incremental_selector());
            auto next_token = sg->main_compaction_group()->next_compaction_group_token();
            _lowest_next_position = next_token.is_maximum()
                    ? _lowest_next_position = dht::ring_position_ext::max()
                    : dht::ring_position_ext::starting_at(next_token);
        }

        auto res = _cur_selector->select(pos);
        // Return all sstables selected on the requested position from the first matching sstable set.
        // This assumes that the underlying sstable sets are disjoint in their token ranges so
        // only one of them contain any given token.
        auto sstables = std::move(res.sstables);
        // Return the lowest next position, such that this function will be called again to select the
        // lowest next position from the selector which previously returned it.
        // Until the current selector is exhausted. In that case,
        // jump to the next compaction_group sstable set.
        dht::ring_position_ext lowest_next_position = res.next_position;
        if (lowest_next_position.is_max()) {
            _cur_set = {};
            _cur_selector.reset();
            std::swap(lowest_next_position, _lowest_next_position);
        }

        tlogger.trace("table_incremental_selector: select pos={}: returning {} sstables, next_pos={}", pos, sstables.size(), lowest_next_position);
        return std::make_tuple(std::move(current_range), std::move(sstables), std::move(lowest_next_position));
    }
};

sstables::sstable_set_impl::selector_and_schema_t table_sstable_set::make_incremental_selector() const {
    return std::make_tuple(std::make_unique<table_incremental_selector>(_table), *_table.schema());
}

flat_mutation_reader_v2
table_sstable_set::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstable_predicate& predicate) const {
    // The singular partition_range start bound must be engaged.
    auto token = pr.start()->value().token();
    auto* sg = _table.storage_group_for_token(token);
    auto set = sg->make_compound_sstable_set();
    return set->create_single_key_sstable_reader(cf, std::move(schema), std::move(permit), sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate);
}

lw_shared_ptr<sstables::sstable_set> table::make_table_sstable_set() {
    return make_lw_shared(table_sstable_set::make(*this));
}

} // namespace replica
