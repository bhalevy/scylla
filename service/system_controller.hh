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
    using callback_type = std::function<future<>()>;

    class subscription : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        callback_type _callback;
    public:
        subscription(callback_type callback) noexcept : _callback(std::move(callback)) {}

        future<> operator()() {
            return _callback();
        }
    };

private:
    using subscription_list_type = bi::list<subscription, bi::constant_time_size<false>>;
    subscription_list_type _isolate_subscribers = subscription_list_type();

public:
    system_controller() = default;
    explicit system_controller(abort_source&);

    abort_source& get_abort_source() noexcept {
        return _abort_source;
    }

    // subscribe callback to be called on isolate()
    subscription on_isolate(callback_type cb);

    // isolate subscribers on all shards
    future<> isolate(sstring reason) noexcept;

private:
    void local_shutdown() noexcept;
};

} // namespace service
