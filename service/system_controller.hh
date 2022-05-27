/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>

#include <boost/intrusive/list.hpp>

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

using namespace seastar;
namespace bi = boost::intrusive;

namespace service {

class system_controller : public peering_sharded_service<system_controller> {
    optimized_optional<abort_source::subscription> _sub;
    abort_source _abort_source;

public:
    system_controller() = default;
    explicit system_controller(abort_source&);

    abort_source& get_abort_source() noexcept {
        return _abort_source;
    }

private:
    void local_shutdown() noexcept;
};

} // namespace service
