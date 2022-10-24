/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace seastar {

class sharded_abort_source  {
    sharded<abort_source> _abort_sources;
    bool _abort_requested = false;
public:
    sharded_abort_source() = default;

    future<> start() {
        return _abort_sources.start();
    }

    future<> stop() noexcept {
        return _abort_sources.stop();
    }

    abort_source& local() noexcept {
        return _abort_sources.local();
    }

    future<> request_abort(std::exception_ptr ex = nullptr) noexcept {
        assert(!std::exchange(_abort_requested, true));
        // _abort_done is waited on in stop()
        return _abort_sources.invoke_on_all([ex = std::move(ex)] (abort_source& as) {
            ex ? as.request_abort_ex(ex) : as.request_abort();
        });
    }

    bool abort_requested() const noexcept {
        return _abort_requested;
    }
};

} // namespace seastar
