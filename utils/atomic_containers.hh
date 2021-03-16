/*
 * Copyright (C) 2020 ScyllaDB
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

#include <seastar/core/rwlock.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/coroutine.hh>

#include <vector>
#include <unordered_set>

// This class supports atomic removes (by using a lock and returning a
// future) and non atomic insert and iteration (by using indexes).
template <typename T>
class atomic_vector {
    std::vector<T> _vec;
    seastar::rwlock _vec_lock;

public:
    void add(const T& value) {
        _vec.push_back(value);
    }
    seastar::future<> remove(const T& value) {
        return with_lock(_vec_lock.for_write(), [this, value] {
            _vec.erase(std::remove(_vec.begin(), _vec.end(), value), _vec.end());
        });
    }

    // This must be called on a thread. The callback function must not
    // call remove.
    //
    // We would take callbacks that take a T&, but we had bugs in the
    // past with some of those callbacks holding that reference past a
    // preemption.
    void for_each(seastar::noncopyable_function<void(T)> func) {
        _vec_lock.for_read().lock().get();
        auto unlock = seastar::defer([this] {
            _vec_lock.for_read().unlock();
        });
        // We grab a lock in remove(), but not in add(), so we
        // iterate using indexes to guard against the vector being
        // reallocated.
        for (size_t i = 0, n = _vec.size(); i < n; ++i) {
            func(_vec[i]);
        }
    }
};

// This class supports atomic removes (by using a lock and returning a
// future) and non atomic insert and iteration (by using indexes).
template <typename T>
class atomic_unordered_set {
    std::unordered_set<T> _set;
    std::unordered_set<T> _pending;
    seastar::rwlock _lock;

public:
    using size_type = typename std::unordered_set<T>::size_type;

    bool insert(const T& value) {
        // If the lock can be acquired, insert directly
        // to `_set`. Otherwise, insert to `_pending`
        // that will be merged into _set by `for_each`.
        if (_lock.try_write_lock()) {
            auto [it, inserted] = _set.insert(value);
            return inserted;
        } else {
            if (_set.contains(value)) {
                return false;
            }
            auto [it, inserted] = _pending.insert(value);
            return inserted;
        }
    }

    seastar::future<size_type> erase(const T& value) noexcept {
        return with_lock(_lock.for_write(), [this, &value] {
            return _pending.erase(value) + _set.erase(value);
        });
    }

    std::unordered_set<T>& set() const noexcept {
        return _set;
    }

    // This must be called on a thread. The callback function must not
    // call erase.
    //
    // We would take callbacks that take a T&, but we had bugs in the
    // past with some of those callbacks holding that reference past a
    // preemption.
    void for_each(seastar::noncopyable_function<void(T)> func) {
        _lock.for_read().lock().get();
        auto unlock = seastar::defer([this] {
            _lock.for_read().unlock();
        });
        assert(_pending.empty());
        auto it = _set.begin();
        while (it != _set.end()) {
            func(*it++);
        }
        if (!_pending.empty()) {
            _set.merge(_pending);
            assert(_pending.empty());
        }
    }

    future<> do_for_each(seastar::noncopyable_function<future<>(T)> func) noexcept {
        return with_lock(_lock.for_read(), [this, func = std::move(func)] () mutable {
            assert(_pending.empty());
            return do_for_each(_set, [func = std::move(func)] (const T& v) mutable {
                return func(v);
            }).finally([this] {
                if (!_pending.empty()) {
                    _set.merge(_pending);
                    assert(_pending.empty());
                }
            });
        });
    }
};
