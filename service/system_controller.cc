/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service/system_controller.hh"
#include "log.hh"

namespace service {

logging::logger sclogger("system_controller");

system_controller::system_controller(abort_source& as)
    : _sub(as.subscribe([this] () noexcept { local_shutdown(); }))
{ }

void system_controller::local_shutdown() noexcept {
    sclogger.info("Shutting down");
    _abort_source.request_abort();
}

system_controller::subscription system_controller::on_isolate(callback_type cb) {
    auto sub = subscription(std::move(cb));
    _isolate_subscribers.push_back(sub);
    return sub;
}

future<> system_controller::isolate(sstring reason) noexcept {
    sclogger.warn("Isolate node due to {}", reason);
    return container().invoke_on_all([] (system_controller& sc) {
        return do_for_each(sc._isolate_subscribers, [] (subscription& sub) {
            return sub();
        });
    });
}

} // namespace service
