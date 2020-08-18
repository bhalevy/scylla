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

#include <exception>

#include <fmt/format.h>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include "seastarx.hh"

namespace utils {

using versioned_shared_ptr_version_type = unsigned;

class versioned_shared_ptr_uid_mismatch_error : public std::runtime_error {
    using version_type = versioned_shared_ptr_version_type;
public:
    explicit versioned_shared_ptr_uid_mismatch_error(version_type shard1, version_type uid1, version_type shard2, version_type uid2)
        : std::runtime_error(fmt::format("versioned_shared_ptr shard/uid mismatch: {}:{} != {}:{}", shard1, uid1, shard2, uid2))
    { }
};

class versioned_shared_ptr_version_mismatch_error : public std::runtime_error {
    using version_type = versioned_shared_ptr_version_type;
public:
    explicit versioned_shared_ptr_version_mismatch_error(version_type version1, version_type version2)
        : std::runtime_error(fmt::format("versioned_shared_ptr version mismatch: {} != {}", version1, version2))
    { }
};

template <typename T>
class versioned_shared_ptr {
    using version_type = versioned_shared_ptr_version_type;

    lw_shared_ptr<T> _ptr;
    shard_id _shard;
    version_type _uid;
    version_type _version;

    explicit versioned_shared_ptr(lw_shared_ptr<T> ptr, version_type uid, version_type version) noexcept
        : _ptr(std::move(ptr))
        , _shard(this_shard_id())
        , _uid(uid)
        , _version(version)
    { }

public:
    versioned_shared_ptr() noexcept
        : _ptr()
        , _shard(this_shard_id())
        , _uid(0)
        , _version(0)
    { }

    versioned_shared_ptr(const versioned_shared_ptr& x) noexcept
        : versioned_shared_ptr(x._ptr, x._uid, x._version)
    { }

    versioned_shared_ptr(versioned_shared_ptr&& x) noexcept
        : versioned_shared_ptr(std::move(x._ptr), x._uid, x._version)
    {
        x._uid = 0;
        x._version = 0;
    }

    versioned_shared_ptr(lw_shared_ptr<T>&& ptr) noexcept
        : versioned_shared_ptr(std::move(ptr), generate_uid(), 1)
    { }

    // may throw uid_mismatch error
    versioned_shared_ptr& operator=(const versioned_shared_ptr& x) {
        set(x);
        return *this;
    }

    // may throw uid_mismatch error
    versioned_shared_ptr& operator=(versioned_shared_ptr&& x) {
        set(std::move(x));
        return *this;
    }

    bool operator==(const versioned_shared_ptr& x) const noexcept {
        return _uid == x._uid && _version == x._version && _ptr == x._ptr;
    }
    bool operator!=(const versioned_shared_ptr& x) const noexcept {
        return !(*this == x);
    }

    explicit operator bool() const noexcept {
        return bool(_ptr);
    }

    T& operator*() const noexcept { return *get(); }
    T* operator->() const noexcept { return get(); }

    T* get() const noexcept {
        return _ptr.get();
    }

    versioned_shared_ptr clone() const {
        return versioned_shared_ptr(make_lw_shared<T>(*(_ptr.get())), _uid, _version + 1);
    }

    shard_id shard() const noexcept {
        return _shard;
    }

    version_type uid() const noexcept {
        return _uid;
    }

    version_type version() const noexcept {
        return _version;
    }

    // may throw uid_mismatch error
    void set(const versioned_shared_ptr& x) {
        check_source(x._ptr, x._uid, x._version);
        check_uid(x._shard, x._uid);
        _ptr = x._ptr;
        _uid = x._uid;
        _version = x._version;
    }

    // may throw uid_mismatch error
    void set(versioned_shared_ptr&& x) {
        check_source(x._ptr, x._uid, x._version);
        check_uid(x._shard, x._uid);
        _ptr = std::move(x._ptr);
        _uid = x._uid;
        _version = x._version;
        x._uid = 0;
        x._version = 0;
    }

    // may throw uid/version_mismatch errors
    void cmp_and_set(const versioned_shared_ptr& x) {
        check_source(x._ptr, x._uid, x._version);
        check_uid(x._shard, x._uid);
        check_version(x._version - 1);
        _ptr = x._ptr;
        _uid = x._uid;
        _version = x._version;
    }

    // may throw uid/version_mismatch errors
    void cmp_and_set(versioned_shared_ptr&& x) {
        check_source(x._ptr, x._uid, x._version);
        check_uid(x._shard, x._uid);
        check_version(x._version - 1);
        _ptr = std::move(x._ptr);
        _uid = x._uid;
        _version = x._version;
        x._uid = 0;
        x._version = 0;
    }

    // reset the data, uid, and version
    void reset() noexcept {
        this->~versioned_shared_ptr();
        new (this) versioned_shared_ptr();
    }
private:
    static version_type generate_uid() {
        static thread_local version_type counter = 0;
        return ++counter;
    }

    void check_source(const lw_shared_ptr<T>& ptr, version_type uid, version_type version) {
        if (ptr) {
            assert(uid && version);
        } else {
            assert(!uid && !version);
        }
    }

    // may throw uid_mismatch error
    void check_uid(shard_id other_shard, version_type other_uid) {
        if (_uid && (_shard != other_shard || _uid != other_uid)) {
            throw versioned_shared_ptr_uid_mismatch_error(_shard, _uid, other_shard, other_uid);
        }
    }

    // may throw version_mismatch error
    void check_version(version_type other_version) {
        if (_version && _version != other_version) {
            throw versioned_shared_ptr_version_mismatch_error(_version, other_version);
        }
    }
};

template <typename T, typename... Args>
versioned_shared_ptr<T> make_versioned_shared_ptr(Args... args) {
    return versioned_shared_ptr<T>(make_lw_shared<T>(std::forward<Args>(args)...));
}

} // namespace utils
