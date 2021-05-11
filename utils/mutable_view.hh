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

enum class mutable_view { no, yes, };

template <typename CharT, mutable_view is_mutable>
class basic_view_base {
public:
    using value_type = CharT;
    using pointer = std::conditional_t<is_mutable == mutable_view::yes, CharT*, const CharT*>;
    using reference = std::conditional_t<is_mutable == mutable_view::yes, CharT&, const CharT&>;
    using iterator = pointer;
    using const_iterator = const CharT*;
private:
    pointer _begin = nullptr;
    pointer _end = nullptr;
public:
    basic_view_base() = default;

    template <typename U, U N, bool NulTerminate>
    basic_view_base(basic_sstring<CharT, U, N, NulTerminate>& str) noexcept
        : _begin(str.begin())
        , _end(str.end())
    { }

    basic_view_base(pointer ptr, size_t length) noexcept
        : _begin(ptr)
        , _end(ptr + length)
    { }

    operator std::basic_string_view<CharT>() const noexcept {
        return std::basic_string_view<CharT>(begin(), size());
    }

    reference operator[](size_t idx) const noexcept {
        assert(idx < size());
        return _begin[idx];
    }

    const CharT& at(size_t idx) const {
        if (idx >= size()) {
            throw std::out_of_range(format("basic_view_base::at: idx (which is {}) >= this->size() (which is {})", idx, size()));
        }
        return _begin[idx];
    }

    iterator begin() const noexcept { return _begin; }
    iterator end() const noexcept { return _end; }

    const_iterator cbegin() const noexcept { return _begin; }
    const_iterator cend() const noexcept { return _end; }

    pointer data() const noexcept { return _begin; }
    size_t size() const noexcept { return _end - _begin; }
    bool empty() const noexcept { return _begin == _end; }

    template <typename>
    requires ( is_mutable == mutable_view::yes )
    CharT& front() noexcept {
        assert(!empty());
        return *_begin;
    }

    const CharT& front() const noexcept {
        assert(!empty());
        return *_begin;
    }

    void remove_prefix(size_t n) noexcept {
        if (__builtin_expect(n <= size(), true)) {
            _begin += n;
        } else {
            _begin = _end;
        }
    }

    void remove_suffix(size_t n) noexcept {
        if (__builtin_expect(n <= size(), true)) {
            _end -= n;
        } else {
            _end = _begin;
        }
    }

    basic_view_base substr(size_t pos, size_t count) noexcept {
        assert(pos <= size());
        size_t n = std::min(count, (_end - _begin) - pos);
        return basic_view_base{_begin + pos, n};
    }
};

template <typename CharT>
using basic_mutable_view = basic_view_base<CharT, mutable_view::yes>;
