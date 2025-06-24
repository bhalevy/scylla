/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <exception>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/coroutine/switch_to.hh>

#include "utils/background_disposer.hh"
#include "utils/assert.hh"
#include "utils/log.hh"

namespace utils {

static logging::logger logger("background_disposer");

background_disposer::basic_item::basic_item(basic_item&& o) noexcept {
    if (_shard != o._shard) {
        on_fatal_internal_error(logger, format("background_disposer::basic_item[{}]: cannot move from basic_item shard={} to this shard", _shard, o._shard));
    }
}

background_disposer::basic_item::~basic_item() {
    if (this_shard_id() != _shard) {
        on_fatal_internal_error(logger, format("background_disposer::basic_item[{}]: detroyed on wrong shard", _shard));
    }
}

background_disposer::background_disposer(std::optional<scheduling_group> sg_opt) {
    _done = disposer(sg_opt);
}

background_disposer::background_disposer(background_disposer&& o) noexcept {
    if (_shard != o._shard) {
        on_fatal_internal_error(logger, format("background_disposer[{}]: cannot move from disposer shard={} to this shard", _shard, o._shard));
    }
    _done = std::exchange(o._done, make_ready_future());
}

background_disposer::~background_disposer() {
    if (!_items.empty()) {
        on_fatal_internal_error(logger, format("background_disposer[{}]: destroyed with non-empty items list", _shard));
    }
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
    logger.debug("dispose: queuing item={} for disposal", fmt::ptr(item.get()));
    if (this_shard_id() != _shard) {
        on_fatal_internal_error(logger,
            format("background_disposer[{}]: cannot dispose item={} on this shard", _shard, fmt::ptr(item.get())));
    }
    if (this_shard_id() != item->owner_shard()) {
        on_fatal_internal_error(logger,
            format("background_disposer[{}]: cannot dispose item={} that was allocated on a foreign shard={}", _shard, fmt::ptr(item.get()), item->owner_shard()));
    }
    // Release the item.  It will be deleted by the disposer loop.
    auto* item_ptr = item.release();
    item_ptr->_disposer_sg = current_scheduling_group();
    _items.push_back(*item_ptr);
    _cond.signal();
}

future<> background_disposer::disposer(std::optional<scheduling_group> sg_opt) noexcept {
    if (sg_opt) {
        co_await coroutine::switch_to(*sg_opt);
    }
    logger.debug("disposer: starting");
    auto saved_sg = current_scheduling_group();
    for (;;) {
        // Always drain all items, even when _stopped.
        while (!_items.empty()) {
            auto& item = _items.front();
            item.unlink();
            logger.debug("disposer: clearing item={}", fmt::ptr(&item));
            try {
                co_await coroutine::switch_to(sg_opt.value_or(item._disposer_sg));
                co_await item.clear_gently();
                co_await coroutine::switch_to(saved_sg);
            } catch (...) {
                logger.warn("disposer: failed to clear_gently item={}: {}.  Will be destroyed anyway", fmt::ptr(&item), std::current_exception());
            }
            logger.debug("disposer: destroying item={}", fmt::ptr(&item));
            std::default_delete<basic_item>()(std::addressof(item));
        }
        if (_stopped) {
            break;
        }
        co_await _cond.wait();
    }
    logger.debug("disposer: done");
}

} // namespace utils
