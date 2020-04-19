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

#include <seastar/util/defer.hh>

#include "atomic_vector.hh"

template <typename T>
seastar::future<> atomic_vector<T>::add(const T& value) {
    return with_lock(_vec_lock.for_write(), [this, &value] {
        _vec.push_back(value);
    });
}

template <typename T>
seastar::future<> atomic_vector<T>::remove(const T& value) {
    return with_lock(_vec_lock.for_write(), [this, value] {
        _vec.erase(std::remove(_vec.begin(), _vec.end(), value), _vec.end());
    });
}

// This must be called on a thread. The callback function must not
// call remove.
template <typename T>
void atomic_vector<T>::for_each(seastar::noncopyable_function<void(T&)> func) {
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
