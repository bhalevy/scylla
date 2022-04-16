/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "replica/apply_mutation.hh"
#include "mutation.hh"
#include "frozen_mutation.hh"
#include "log.hh"

extern logging::logger dblog;

namespace replica {

class database;

apply_mutation::apply_mutation(const mutation& m)
    : _schema(m.schema())
    , _mp(&m)
    , _shard_of(calc_shard_of())
{}

apply_mutation::apply_mutation(mutation&& m)
    : _schema(m.schema())
    , _m(make_foreign(std::make_unique<const mutation>(std::move(m))))
    , _mp(_m.get())
    , _shard_of(calc_shard_of())
{}

apply_mutation::apply_mutation(schema_ptr s, const frozen_mutation& fm)
    : _schema(std::move(s))
    , _fmp(&fm)
    , _shard_of(calc_shard_of())
{}

apply_mutation::apply_mutation(schema_ptr s, frozen_mutation&& fm)
    : _schema(std::move(s))
    , _fm(make_foreign(std::make_unique<const frozen_mutation>(std::move(fm))))
    , _fmp(_fm.get())
    , _shard_of(calc_shard_of())
{}

apply_mutation::apply_mutation(apply_mutation&& o) noexcept // not really noexcept, but required for database::apply_stage
    : _m(std::move(o._m))
    , _mp(std::exchange(o._mp, nullptr))
    , _fm(std::move(o._fm))
    , _fmp(std::exchange(o._fmp, nullptr))
    , _shard_of(std::exchange(o._shard_of, ~0))
{
    if (o._gs) {
        // the global_schema_ptr can be moved only
        // on the same shard it was created.
        if (this_shard_id() == o._gs->_cpu_of_origin) {
            _gs.emplace(std::move(*o._gs));
        } else {
            // otherwise create a new global_schema_ptr by copying the other one
            // note: may throw
            _gs.emplace(*o._gs);
        }
        // update the local _schema from the global_schema_ptr
        _schema = _gs->get();
    } else {
        _schema = std::move(o._schema);
    }
}

apply_mutation::~apply_mutation() {}

unsigned apply_mutation::calc_shard_of() {
    return visit(
        [] (const mutation& m) {
            return dht::shard_of(*m.schema(), m.token());
        },
        [] (schema_ptr s, const frozen_mutation& fm) {
            return dht::shard_of(*s, dht::get_token(*s, fm.key()));
        }
    );
}

void apply_mutation::make_global_schema() {
    if (!_gs) {
        _gs.emplace(_schema);
    }
}

const mutation& apply_mutation::get_mutation() {
    if (!_mp) {
        _m = make_foreign(std::make_unique<const mutation>(_fmp->unfreeze(schema())));
        _mp = _m.get();
    }
    return *_mp;
}

const frozen_mutation& apply_mutation::get_frozen_mutation() {
    if (!_fmp) {
        _fm = make_foreign(std::make_unique<const frozen_mutation>(freeze(*_mp)));
        _fmp = _fm.get();
    }
    return *_fmp;
}

utils::UUID apply_mutation::column_family_id() const noexcept {
    return _mp ? _mp->column_family_id() : _fmp->column_family_id();
}

std::ostream& operator<<(std::ostream& os, const apply_mutation& am) {
    if (am._mp) {
        return os << *am._mp;
    } else {
        return os << am._fmp->pretty_printer(am.schema());
    }
}

} // namespace replica
