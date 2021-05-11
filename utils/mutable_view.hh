/*
 * Copyright (C) 2017 ScyllaDB
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

#include <string_view>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

template<typename CharT>
class basic_mutable_view {
    CharT* _begin = nullptr;
    CharT* _end = nullptr;
public:
    using value_type = CharT;
    using pointer = CharT*;
    using iterator = CharT*;
    using const_iterator = CharT*;

    basic_mutable_view() = default;

    template<typename U, U N>
    basic_mutable_view(basic_sstring<CharT, U, N>& str) noexcept
        : _begin(str.begin())
        , _end(str.end())
    { }

    basic_mutable_view(CharT* ptr, size_t length) noexcept
        : _begin(ptr)
        , _end(ptr + length)
    { }

    operator std::basic_string_view<CharT>() const noexcept {
        return std::basic_string_view<CharT>(begin(), size());
    }

    CharT& operator[](size_t idx) const noexcept {
        assert(idx < size());
        return _begin[idx];
    }

    const CharT& at(size_t idx) const {
        if (idx >= size()) {
            throw std::out_of_range(format("basic_mutable_view::at: idx (which is {}) >= this->size() (which is {})", idx, size()));
        }
        return _begin[idx];
    }

    iterator begin() const noexcept { return _begin; }
    iterator end() const noexcept { return _end; }

    CharT* data() const noexcept { return _begin; }
    size_t size() const noexcept { return _end - _begin; }
    bool empty() const noexcept { return _begin == _end; }

    CharT& front() noexcept {
        assert(!empty());
        return *_begin;
    }

    const CharT& front() const noexcept {
        assert(!empty());
        return *_begin;
    }

    void remove_prefix(size_t n) noexcept {
        assert(n <= size());
        _begin += n;
    }

    void remove_suffix(size_t n) noexcept {
        assert(n <= size());
        _end -= n;
    }

    basic_mutable_view substr(size_t pos, size_t count) noexcept {
        assert(pos <= size());
        size_t n = std::min(count, (_end - _begin) - pos);
        return basic_mutable_view{_begin + pos, n};
    }
};
