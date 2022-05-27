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

} // namespace service
