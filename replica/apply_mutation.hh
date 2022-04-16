/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>

#include "schema_registry.hh"

class mutation;
class frozen_mutation;

namespace replica {

class apply_mutation {
    schema_ptr _schema;
    std::optional<global_schema_ptr> _gs;
    foreign_ptr<std::unique_ptr<const mutation>> _m;
    const mutation* _mp = nullptr;
    foreign_ptr<std::unique_ptr<const frozen_mutation>> _fm;
    const frozen_mutation* _fmp = nullptr;
    unsigned _shard_of = ~0;

    unsigned calc_shard_of();

    friend std::ostream& operator<<(std::ostream& os, const apply_mutation& am);
public:
    apply_mutation() = delete;

    // keep reference to a const mutation
    // caller must keep it valid while the mutation is applied
    apply_mutation(const mutation& m);
    // consume mutation
    apply_mutation(mutation&& m);
    // keep reference to a const frozen_mutation
    // caller must keep it valid while the mutation is applied
    apply_mutation(schema_ptr s, const frozen_mutation& fm);
    // consume frozen_mutation
    apply_mutation(schema_ptr s, frozen_mutation&& fm);

    apply_mutation(const apply_mutation&) = delete;
    apply_mutation(apply_mutation&& o) noexcept;

    ~apply_mutation();

    apply_mutation& operator=(const apply_mutation&) = delete;
    apply_mutation& operator=(apply_mutation&&) = default;

    schema_ptr schema() const {
        return _gs ? _gs->get() : _schema;
    }

    // make a global_schema_ptr
    // note: must be called before applying mutation on another db shard.
    void make_global_schema();

    // get a reference to a mutation
    // note: if needed, makes one by unfreezing the frozen_mutation
    const mutation& get_mutation();
    // get a reference to a frozen_mutation
    // note: if needed, makes one by freezing the mutation
    const frozen_mutation& get_frozen_mutation();

    // visit either the available mutation or frozen_mutation,
    // mutation first.
    template <typename MutVisitor, typename FrozenMutVisitor>
    requires std::same_as<
            std::invoke_result_t<MutVisitor, const mutation&>,
            std::invoke_result_t<FrozenMutVisitor, schema_ptr, const frozen_mutation&> >
    auto visit(MutVisitor mut_visitor, FrozenMutVisitor frozen_mut_visitor) {
        return _mp ? mut_visitor(*_mp) : frozen_mut_visitor(schema(), *_fmp);
    }

    // visit either the available mutation or frozen_mutation,
    // frozen_mutation first.
    template <typename FrozenMutVisitor, typename MutVisitor>
    requires std::same_as<
            std::invoke_result_t<FrozenMutVisitor, schema_ptr, const frozen_mutation&>,
            std::invoke_result_t<MutVisitor, const mutation&> >
    auto visit(FrozenMutVisitor frozen_mut_visitor, MutVisitor mut_visitor) {
        return _fmp ? frozen_mut_visitor(schema(), *_fmp) : mut_visitor(*_mp);
    }

    // get the table uuid
    utils::UUID column_family_id() const noexcept;

    // get the owner shard of the mutation
    unsigned shard_of() const noexcept {
        return _shard_of;
    }
};

std::ostream& operator<<(std::ostream& os, const apply_mutation& am);

} // namespace replica
