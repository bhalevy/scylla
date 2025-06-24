/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "seastar/core/shard_id.hh"
#include <list>
#include <memory>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/scheduling.hh>

using namespace seastar;

namespace utils {

class background_disposer {
public:
    class basic_item {
        friend class background_disposer;

        unsigned _shard = this_shard_id();
        scheduling_group _sg;
    public:
        basic_item() = default;
        basic_item(basic_item&&);
        basic_item(const basic_item&) = delete;
        virtual ~basic_item();
        virtual future<> clear_gently() noexcept = 0;
    };
private:
    unsigned _shard = this_shard_id();
    bool _stopped = false;
    future<> _done = make_ready_future();
    condition_variable _cond;
    std::list<std::unique_ptr<basic_item>> _items;
public:
    background_disposer(std::optional<scheduling_group> sg_opt = std::nullopt);
    background_disposer(background_disposer&&);
    background_disposer(const background_disposer&) = delete;
    ~background_disposer();

    future<> stop() noexcept;

    void dispose(std::unique_ptr<basic_item> item) noexcept;

private:
    future<> disposer(std::optional<scheduling_group> sg_opt) noexcept;
};

} // namespace utils
