/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include "utils/phased_barrier.hh"
#include "utils/log.hh"

class service_permit {
public:
    struct data {
        seastar::semaphore_units<> units;
        utils::phased_barrier::operation op;
        sstring desc;
    };
private:
    seastar::lw_shared_ptr<data> _permit;
    service_permit(seastar::semaphore_units<>&& u, utils::phased_barrier::operation op, sstring desc);
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit, sstring desc, utils::phased_barrier::operation op);
    friend service_permit empty_service_permit();
public:
    ~service_permit();
    size_t count() const { return _permit ? _permit->units.count() : 0; };
    operator bool() const noexcept {
        return bool(_permit);
    }
};

inline service_permit make_service_permit(seastar::semaphore_units<>&& permit, sstring desc, utils::phased_barrier::operation op = {}) {
    return service_permit(std::move(permit), std::move(op), std::move(desc));
}

inline service_permit make_service_permit(utils::phased_barrier::operation op, sstring desc) {
    return make_service_permit(seastar::semaphore_units<>(), std::move(desc), std::move(op));
}

inline service_permit empty_service_permit() {
    return make_service_permit(seastar::semaphore_units<>(), "empty_service_permit", utils::phased_barrier::operation());
}
