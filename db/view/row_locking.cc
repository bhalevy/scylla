/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "row_locking.hh"
#include "log.hh"

#include <seastar/core/when_all.hh>
#include <seastar/core/coroutine.hh>

static logging::logger mylog("row_locking");

row_locker::row_locker(schema_ptr s)
    : _schema(s)
    , _two_level_locks(1, decorated_key_hash(), decorated_key_equals_comparator(this))
{
}

void row_locker::upgrade(schema_ptr new_schema) {
    if (new_schema == _schema) {
        return;
    }
    mylog.debug("row_locker::upgrade from {} to {}", fmt::ptr(_schema.get()), fmt::ptr(new_schema.get()));
    _schema = new_schema;
}

row_locker::lock_holder::lock_holder()
    : _locker(nullptr)
    , _partition(nullptr)
    , _partition_exclusive(true)
    , _row(nullptr)
    , _row_exclusive(true) {
}

row_locker::lock_holder::lock_holder(row_locker* locker, const dht::decorated_key* pk, bool exclusive)
    : _locker(locker)
    , _partition(pk)
    , _partition_exclusive(exclusive)
    , _row(nullptr)
    , _row_exclusive(true) {
}

row_locker::lock_holder::lock_holder(row_locker* locker, const dht::decorated_key* pk, const clustering_key_prefix* cpk, bool exclusive)
    : _locker(locker)
    , _partition(pk)
    , _partition_exclusive(false)
    , _row(cpk)
    , _row_exclusive(exclusive) {
}

row_locker::latency_stats_tracker::latency_stats_tracker(row_locker::single_lock_stats& stats)
    : lock_stats(stats)
{
    waiting_latency.start();
    lock_stats.operations_currently_waiting_for_lock++;
}

row_locker::latency_stats_tracker::~latency_stats_tracker() {
    lock_stats.operations_currently_waiting_for_lock--;
    waiting_latency.stop();
    lock_stats.estimated_waiting_for_lock.add(waiting_latency.latency());
}

void row_locker::latency_stats_tracker::lock_acquired() {
    lock_stats.lock_acquisitions++;
}

future<row_locker::lock_holder>
row_locker::lock_pk(const dht::decorated_key& pk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking {} lock on entire partition {}", (exclusive ? "exclusive" : "shared"), pk);
    auto tracker = latency_stats_tracker(exclusive ? stats.exclusive_partition : stats.shared_partition);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    auto f = exclusive ? i->second._partition_lock.write_lock(timeout) : i->second._partition_lock.read_lock(timeout);
    co_await std::move(f);
    // Note: we rely on the fact that &i->first, the pointer to a key, never
    // becomes invalid (as long as the item is actually in the hash table),
    // even in the case of rehashing.
    tracker.lock_acquired();
    co_return lock_holder(this, &i->first, exclusive);
}

future<row_locker::lock_holder>
row_locker::lock_ck(const dht::decorated_key& pk, const clustering_key_prefix& cpk, bool exclusive, db::timeout_clock::time_point timeout, stats& stats) {
    mylog.debug("taking shared lock on partition {}, and {} lock on row {} in it", pk, (exclusive ? "exclusive" : "shared"), cpk);
    auto tracker = latency_stats_tracker(exclusive ? stats.exclusive_row : stats.shared_row);
    auto i = _two_level_locks.try_emplace(pk, this).first;
    auto partition_lock_holder = co_await i->second._partition_lock.hold_read_lock(timeout);
    auto j = i->second._row_locks.find(cpk);
    if (j == i->second._row_locks.end()) {
        // Not yet locked, need to create the lock. This makes a copy of cpk.
        j = i->second._row_locks.emplace(cpk, lock_type()).first;
    }
    auto row_lock_holder = co_await (exclusive ? j->second.hold_write_lock(timeout) : j->second.hold_read_lock(timeout));
    tracker.lock_acquired();
    partition_lock_holder.release();
    row_lock_holder.release();
    co_return lock_holder(this, &i->first, &j->first, exclusive);
}

row_locker::lock_holder::lock_holder(row_locker::lock_holder&& old) noexcept
        : _locker(old._locker)
        , _partition(old._partition)
        , _partition_exclusive(old._partition_exclusive)
        , _row(old._row)
        , _row_exclusive(old._row_exclusive)
{
    // We also need to zero old's _partition and _row, so when destructed
    // the destructor will do nothing and further moves will not create
    // duplicates.
    old._partition = nullptr;
    old._row = nullptr;
}

row_locker::lock_holder& row_locker::lock_holder::operator=(row_locker::lock_holder&& old) noexcept {
    if (this != &old) {
        this->~lock_holder();
        _locker = old._locker;
        _partition = old._partition;
        _partition_exclusive = old._partition_exclusive;
        _row = old._row;
        _row_exclusive = old._row_exclusive;
        // As above, need to also zero other's data
        old._partition = nullptr;
        old._row = nullptr;
    }
    return *this;
}

void
row_locker::unlock(const dht::decorated_key* pk, bool partition_exclusive,
                    const clustering_key_prefix* cpk, bool row_exclusive) {
    // Look for the partition and/or row locks given keys, release the locks,
    // and if nobody is using one of lock objects any more, delete it:
    if (pk) {
        auto pli = _two_level_locks.find(*pk);
        if (pli == _two_level_locks.end()) {
            // This shouldn't happen... We can't unlock this lock if we can't find it...
            mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for partition", *pk);
            return;
        }
        assert(&pli->first == pk);
        if (cpk) {
            auto rli = pli->second._row_locks.find(*cpk);
            if (rli == pli->second._row_locks.end()) {
                mylog.error("column_family::local_base_lock_holder::~local_base_lock_holder() can't find lock for row", *cpk);
                return;
            }
            assert(&rli->first == cpk);
            mylog.debug("releasing {} lock for row {} in partition {}", (row_exclusive ? "exclusive" : "shared"), *cpk, *pk);
            auto& lock = rli->second;
            if (row_exclusive) {
                lock.write_unlock();
            } else {
                lock.read_unlock();
            }
            if (!lock.locked()) {
                mylog.debug("Erasing lock object for row {} in partition {}", *cpk, *pk);
                pli->second._row_locks.erase(rli);
            }
        }
        mylog.debug("releasing {} lock for entire partition {}", (partition_exclusive ? "exclusive" : "shared"), *pk);
        auto& lock = pli->second._partition_lock;
        if (partition_exclusive) {
            lock.write_unlock();
        } else {
            lock.read_unlock();
        }
        if (!lock.locked()) {
            auto& row_locks = pli->second._row_locks;
            // We can erase the partition entry only if all its rows are unlocked.
            // We don't expect any locked rows since the lock_holder is supposed
            // to clean up any locked row by calling this function in its dtor.
            // However, some unlocked rows may remain if lock_holder::lock_row
            // failed to acquire the row after the row was created.
            if (!row_locks.empty()) {
                for (auto it = row_locks.begin(); it != row_locks.end(); ) {
                    if (it->second.locked()) {
                        mylog.warn("Encounered a locked row {} when unlocking partition {}", it->first, *pk);
                        ++it;
                    } else {
                        it = row_locks.erase(it);
                    }
                }
            }
            if (row_locks.empty()) {
                mylog.debug("Erasing lock object for partition {}", *pk);
                _two_level_locks.erase(pli);
            }
        }
     }
}

row_locker::lock_holder::~lock_holder() {
    if (_locker) {
        _locker->unlock(_partition,  _partition_exclusive, _row, _row_exclusive);
    }
}
