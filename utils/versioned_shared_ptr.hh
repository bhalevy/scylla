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

// versioned_shared_ptr<T> is used to gain controlled access to
// a shared object of type T.
//
// Like {lw_,}shared_ptr<T>, versioned_shared_ptr<T> provides the
// usual seastar::shared_ptr construct, copy, move, and assignment
// methods, as well as the *, ->, and bool operators.
//
// On top of that, versioned_shared_ptr<T> has the following attributes:
// shard(), uid(), and version() provding unique identification within
// a shard, and a running version of a "lineage" of changes to the
// versioned_shared_ptr.
//
// Non-preemptive readers can keep using a const T& directly.
// Readers that may yield should get a const copy of versioned_shared_ptr<T>.
//
// To safely modify T, the user calls clone() that returns a
// versioned_shared_ptr<T> pointing to a copy of T, that the user
// can modify without affecting any readers.  Eventually, set()
// or cmp_and_set() can be called to "commit" the changes back,
// while checking that the shard and uid match, and with cmp_and_set
// also checking that the version matches, to enforce serialization
// of changes to the shared object.
//
// reset() can be used to set the versioned_shared_ptr<T> to
// an empty state, equivalent to the default-constructed state,
// into which any value can be set.
//
// versioned_shared_ptr<T> is intended to be used via
// a versioned_shared_object<T> that holds the base copy
// of the object T. See details below.

using versioned_shared_ptr_version_type = unsigned;

class versioned_shared_ptr_uid_mismatch_error : public std::runtime_error {
    using version_type = versioned_shared_ptr_version_type;
public:
    explicit versioned_shared_ptr_uid_mismatch_error(shard_id shard1, version_type uid1, shard_id shard2, version_type uid2)
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

    lw_shared_ptr<T> _ptr;  // pointer to the shard object of type T
    shard_id _shard;        // shard that generated the uid
    version_type _uid;      // unique identifier within the shard
    version_type _version;  // running version of the object T pointed by _ptr

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

    // return a versioned_shared_ptr<T> to a cloned copy of the object T
    // marked with the consecutive version, that can be verified later
    // with cmp_and_set().
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

    // set a new value of the object T
    // checking only the shard/uid pair of the lineage.
    // After reset() any value can be set.
    //
    // may throw uid_mismatch error if x's shard/uid don't match this one's
    void set(const versioned_shared_ptr& x) {
        check_source(x._ptr, x._uid, x._version);
        check_uid(x._shard, x._uid);
        _ptr = x._ptr;
        _uid = x._uid;
        _version = x._version;
    }

    // may throw uid_mismatch error if x's shard/uid don't match this one's
    void set(versioned_shared_ptr&& x) {
        check_source(x._ptr, x._uid, x._version);
        check_uid(x._shard, x._uid);
        _ptr = std::move(x._ptr);
        _uid = x._uid;
        _version = x._version;
        x._uid = 0;
        x._version = 0;
    }

    // set a new value of the object T
    // checking the shard/uid of the lineage
    // as well as the expected version.
    // After reset() any value can be set.
    //
    // may throw uid_mismatch error if x's shard/uid don't match this one's
    // may throw version_mismatch error if x.s version is not equal to the current version+1
    void cmp_and_set(const versioned_shared_ptr& x) {
        check_source(x._ptr, x._uid, x._version);
        check_uid(x._shard, x._uid);
        check_version(x._version - 1);
        _ptr = x._ptr;
        _uid = x._uid;
        _version = x._version;
    }

    // may throw uid_mismatch error if x's shard/uid don't match this one's
    // may throw version_mismatch error if x.s version is not equal to the current version+1
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

// versioned_shared_object<T> points to a shared object of type T
// that is constructed by the versioned_shared_object<T> (default)
// constructor.  This can be particularly useful for implementing
// sharded<versioned_shared_object<T>> where T is constructed in
// sharded::start().
//
// Non-preemptive readers can keep using get() to get a const T& directly.
// Readers that may yield should get a const versioned_shared_ptr<T>
// using get_shared_ptr().  version() can be used to compare the version
// of the versioned_shared_object to the snapshot got by get_shared_ptr()
// to see if the base object may have changed while holding the snapshot.
//
// To safely modify T, the user calls clone_shared_ptr() that returns a
// versioned_shared_ptr<T> pointing to a copy of T, that the user
// can modify without affecting any readers.  Eventually, set_shared_ptr()
// or cmp_and_set_shared_ptr() can be called to "commit" the changes back,
// while checking that the shard and uid match, and with cmp_and_set_shared_ptr
// also checking that the version matches, to enforce serialization
// of changes to the shared object.

template <typename T>
class versioned_shared_object {
    lw_shared_ptr<versioned_shared_ptr<T>> _shared;
public:
    // used to construct the shared object as sharded<> instance
    template <typename... Args>
    versioned_shared_object(Args... args) noexcept(std::is_nothrow_constructible_v<T, Args...>)
        : _shared(make_lw_shared<versioned_shared_ptr<T>>(make_lw_shared<T>(std::forward<Args>(args)...)))
    { }

    versioned_shared_object(const versioned_shared_object&) noexcept = default;

    versioned_shared_object(versioned_shared_object&&) noexcept(std::is_nothrow_move_constructible_v<T>) = default;

    auto version() const noexcept {
        return _shared->version();
    }

    // get a read-only snapshot of the shared object,
    // guaranteed not to change while holding the versioned_shared_ptr.
    //
    // Note: caller may use version() to detect changes in the
    // versioned_shared_object.
    const versioned_shared_ptr<T> get_shared_ptr() const noexcept {
        return *_shared;
    }

    // get a mutable snapshot of the shared object.
    // can be modified without affecting anyone who got
    // a snapshot using get_shared_ptr().
    //
    // when done, caller can apply the changes to the shared object
    // using cmp_and_set_shared_ptr(), or set_shared_ptr() (if serializability
    // of changes isn't required).
    versioned_shared_ptr<T> clone_shared_ptr() const {
        return _shared->clone();
    }

    // Safely apply changes to the shared object done on a versioned_shared_ptr
    // previously retrieved by clone_shared_ptr().
    //
    // Exceptions: may throw uid/version_mismatch errors
    void cmp_and_set_shared_ptr(const versioned_shared_ptr<T>& x) {
        _shared->cmp_and_set(x);
    }

    // Safely apply changes to the shared object done on a versioned_shared_ptr
    // previously retrieved by clone_shared_ptr().
    //
    // Exceptions: may throw uid/version_mismatch errors
    void cmp_and_set_shared_ptr(versioned_shared_ptr<T>&& x) {
        _shared->cmp_and_set(std::move(x));
    }

    // apply changes to the shared object done on a versioned_shared_ptr
    // previously retrieved by clone_shared_ptr().
    //
    // Exceptions: may throw uid_mismatch error
    void set_shared_ptr(const versioned_shared_ptr<T>& x) {
        _shared->set(x);
    }

    // apply changes to the shared object done on a versioned_shared_ptr
    // previously retrieved by clone_shared_ptr().
    //
    // Exceptions: may throw uid_mismatch error
    void set_shared_ptr(versioned_shared_ptr<T>&& x) {
        _shared->set(std::move(x));
    }

    // get a reference to the shared object
    // caller must not yield when using this reference
    const T& get() const noexcept {
        return **_shared;
    }
};

} // namespace utils
