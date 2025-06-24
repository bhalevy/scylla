/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <memory>
#include <optional>

#include <boost/intrusive/list.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/scheduling.hh>

using namespace seastar;

namespace utils {

class background_disposer {
public:
    class basic_item : public boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> {
        unsigned _shard = this_shard_id();
        scheduling_group _disposer_sg;

        friend class background_disposer;
    public:
        basic_item() = default;
        basic_item(basic_item&&) noexcept;
        basic_item(const basic_item&) = delete;
        virtual ~basic_item();

        unsigned owner_shard() const noexcept { return _shard; };

        virtual future<> clear_gently() noexcept = 0;
    };

private:
    using inrusive_list_type = bi::list<basic_item,
        boost::intrusive::base_hook<boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>>,
        boost::intrusive::constant_time_size<false>>;

    unsigned _shard = this_shard_id();
    bool _stopped = false;
    future<> _done = make_ready_future();
    condition_variable _cond;
    inrusive_list_type _items;
public:
    background_disposer(std::optional<scheduling_group> sg_opt = std::nullopt);
    background_disposer(background_disposer&&) noexcept;
    background_disposer(const background_disposer&) = delete;
    ~background_disposer();

    future<> stop() noexcept;

    void dispose(std::unique_ptr<basic_item> item) noexcept;

private:
    future<> disposer(std::optional<scheduling_group> sg_opt) noexcept;
};

} // namespace utils
