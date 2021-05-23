/*
 * Copyright 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/shared_future.hh>
#include "seastarx.hh"

namespace utils {

// Synchronizer which allows to track and wait for asynchronous operations
// which were in progress at the time of wait initiation.
class phased_barrier {
public:
    using phase_type = uint64_t;
private:
    using gate = seastar::gate;
    lw_shared_ptr<gate> _gate;
    shared_future<> _pending;
    phase_type _phase;
public:
    phased_barrier()
        : _gate(make_lw_shared<gate>())
        , _phase(0)
    { }

    class operation {
        lw_shared_ptr<gate> _gate;
    public:
        operation() : _gate() {}
        operation(lw_shared_ptr<gate> g) : _gate(std::move(g)) {}
        operation(const operation&) = delete;
        operation(operation&&) = default;
        operation& operator=(operation&& o) noexcept {
            if (this != &o) {
                this->~operation();
                new (this) operation(std::move(o));
            }
            return *this;
        }
        ~operation() {
            if (_gate) {
                _gate->leave();
            }
        }
    };

    // Starts new operation. The operation ends when the "operation" object is destroyed.
    // The operation may last longer than the life time of the phased_barrier.
    operation start() {
        if (!_gate) {
            _gate = make_lw_shared<gate>();
        }
        _gate->enter();
        return { _gate };
    }

    // Starts a new phase and waits for all operations started in any of the earlier phases.
    // It is fine to start multiple awaits in parallel.
    // Never fails.
    future<> advance_and_await() noexcept {
        if (_pending.valid()) {
            return _pending;
        }

        if (!_gate) {
            return make_ready_future<>();
        }

        shared_promise<> pr;
        _pending = pr.get_shared_future();
        future<> ret = _pending;
        ++_phase;
        auto old_gate = std::move(_gate);
        // run in background, synchronize using `ret`
        (void)old_gate->close().then_wrapped([this, old_gate = std::move(old_gate), pr = std::move(pr)] (future<> f) mutable {
            if (f.failed()) {
                pr.set_exception(f.get_exception());
            } else {
                pr.set_value();
            }
            _pending = {};
        });
        return std::move(ret);
    }

    // Returns current phase number. The smallest value returned is 0.
    phase_type phase() const {
        return _phase;
    }

    // Number of operations in current phase.
    size_t operations_in_progress() const {
        return _gate ? _gate->get_count() : 0;
    }
};

}
