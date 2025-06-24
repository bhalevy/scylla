/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shard_id.hh>

#include "utils/background_disposer.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

namespace utils {

static logging::logger logger("background_disposer");

background_disposer::basic_item::basic_item(basic_item&& o) {
    if (_shard != o._shard) {
        on_fatal_internal_error(logger, format("background_disposer::basic_item[{}]: cannot move from basic_item shard={} to this shard", _shard, o._shard));
    }
}

background_disposer::basic_item::~basic_item() {
    if (this_shard_id() != _shard) {
        on_fatal_internal_error(logger, format("background_disposer::basic_item[{}]: detroyed on wrong shard", _shard));
    }
}

background_disposer::background_disposer() {
    _done = disposer();
}

background_disposer::background_disposer(background_disposer&& o) {
    if (_shard != o._shard) {
        on_fatal_internal_error(logger, format("background_disposer[{}]: cannot move from disposer shard={} to this shard", _shard, o._shard));
    }
    _done = std::exchange(o._done, make_ready_future());
}

background_disposer::~background_disposer() {
    SCYLLA_ASSERT(_stopped);
}

future<> background_disposer::stop() noexcept {
    if (this_shard_id() != _shard) {
        on_fatal_internal_error(logger, format("background_disposer[{}]: cannot stop disposer on this shard", _shard));
    }
    logger.debug("disposer: stopping");
    _stopped = true;
    _cond.signal();
    co_await std::exchange(_done, make_ready_future());
    logger.debug("disposer: stopped");
}

void background_disposer::dispose(std::unique_ptr<basic_item> item) noexcept {
    if (this_shard_id() != _shard) {
        on_fatal_internal_error(logger, format("background_disposer[{}]: cannot dispose item={} on this shard", _shard, fmt::ptr(item.get())));
    }
    if (this_shard_id() != item->_shard) {
        on_fatal_internal_error(logger, format("background_disposer[{}]: cannot dispose item={} that was allocated on a foreign shard={}", _shard, fmt::ptr(item.get()), item->_shard));
    }
    logger.debug("dispose: queuing item={} for disposal", fmt::ptr(item.get()));
    try {
        _items.push_back(std::move(item));
        _cond.signal();
    } catch (...) {
        // the item will be destroyed synchronously
        logger.warn("dispose: exception while disposing item ignored: {}.", std::current_exception());
    }
}

future<> background_disposer::disposer() noexcept {
    logger.debug("disposer: starting");
    while (!(_items.empty() && _stopped)) {
        if (!_items.empty()) {
            auto& item = _items.front();
            logger.debug("disposer: clearing item={}", fmt::ptr(item.get()));
            co_await item->clear_gently();
            logger.debug("disposer: destroying item={}", fmt::ptr(item.get()));
            _items.pop_front();
        } else {
            co_await _cond.wait();
        }
    }
    logger.debug("disposer: done");
}

} // namespace utils
