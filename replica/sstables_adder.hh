/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "row_cache.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstable_set.hh"

using namespace seastar;

namespace replica {

using enable_backlog_tracker = bool_class<class enable_backlog_tracker_tag>;
using is_main = bool_class<struct is_main_tag>;

class compaction_group;

class compaction_group_sstables_adder : public row_cache::external_updater_impl {
protected:
    compaction_group& _cg;
    std::vector<sstables::shared_sstable> _sstables;
    is_main _main;
private:
    lw_shared_ptr<sstables::sstable_set> _new_sstable_set;
public:
    compaction_group_sstables_adder(compaction_group& cg, std::vector<sstables::shared_sstable> sstables, is_main main);
    // Guarantees strong exception safety
    virtual future<> prepare() override;
    // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
    virtual void execute() override;
};

class table_sstables_adder : public compaction_group_sstables_adder {
protected:
    table& _t;
public:
    table_sstables_adder(table& t, compaction_group& cg, std::vector<sstables::shared_sstable> sstables, is_main main);
    // Guarantees strong exception safety
    virtual future<> prepare() override;
    // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
    virtual void execute() override;
};

} // namespace replica
