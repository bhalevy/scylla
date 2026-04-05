/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/util/closeable.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

#include "utils/on_internal_error.hh"
#include "seastarx.hh"

namespace utils {

template<closeable T>
class shared_ptr_tracker {
    shared_ptr<T> _ptr;
    named_gate::holder _holder;
public:
    shared_ptr_tracker() = default;
    shared_ptr_tracker(std::nullptr_t) noexcept : shared_ptr_tracker() {}
    shared_ptr_tracker(const shared_ptr<T>& ptr, gate::holder holder) noexcept
        : _ptr(ptr)
        , _holder(std::move(holder))
    {}
    shared_ptr_tracker(shared_ptr_tracker&&) = default;
    shared_ptr_tracker& operator=(shared_ptr_tracker&&) = default;
    shared_ptr_tracker& operator=(std::nullptr_t) { return *this = shared_ptr_tracker(); }
    operator bool() const noexcept {
        return static_cast<bool>(_ptr);
    }
    const T& operator*() const noexcept {
        return *_ptr;
    }
    T& operator*() noexcept {
        return *_ptr;
    }
    const T* operator->() const noexcept {
        return _ptr.get();
    }
    T* operator->() noexcept {
        return _ptr.get();
    }
    T* get() const noexcept {
        return _ptr.get();
    }
    void reset() noexcept {
        _ptr = nullptr;
        _holder.release();
    }
};

// Close the shared pointer when the last reference is destroyed.
template<closeable T>
class closeable_shared_ptr_factory {
    shared_ptr<T> _ptr;
    mutable named_gate _gate;
public:
    closeable_shared_ptr_factory(shared_ptr<T> ptr)
        : _ptr(std::move(ptr))
        , _gate(format("closeable_shared_ptr<{}>", typeid(T).name()))
    {}
    closeable_shared_ptr_factory(closeable_shared_ptr_factory&&) = default;
    ~closeable_shared_ptr_factory() {
        if (_ptr) {
            on_internal_error("closeable_shared_ptr destroyed while open");
        }
    }
    shared_ptr_tracker<T> get() const {
        if (!_ptr) {
            on_internal_error("closeable_shared_ptr is already closed");
        }
        return shared_ptr_tracker<T>(_ptr, _gate.hold());
    }

    future<> close() noexcept {
        if (!_ptr) {
            on_internal_error("closeable_shared_ptr is already closed");
        }
        auto ptr = std::exchange(_ptr, nullptr);
        co_await _gate.close();
        co_await ptr->close();
    }
};

} // namespace utils