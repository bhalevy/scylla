/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <utility>
#include <memory>
#include <optional>
#include <concepts>

#include <boost/intrusive/list.hpp>

#include "utils/chunked_vector.hh"
#include "utils/anchorless_list.hh"

namespace utils {

template<typename Key, class T, size_t max_contiguous_allocation = 128*1024,
        typename Hash = std::hash<Key>,
        typename KeyEqual = std::equal_to<Key>,
        typename Allocator = std::allocator<std::pair<const Key, T>>
>
requires (std::is_nothrow_move_constructible_v<Key> && std::is_nothrow_move_constructible_v<T>)
class chunked_unordered_map {
public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, T>;
    using size_type = size_t;
    using difference_type = std::ptrdiff_t;
    using hasher = Hash;
    using key_equal = KeyEqual;
    using allocator_type = Allocator;
    using reference = value_type&;
    using const_reference = const value_type&;
    using pointer = typename std::allocator_traits<Allocator>::pointer;
    using const_pointer = typename std::allocator_traits<Allocator>::const_pointer;

private:
    struct node : public value_type {
        boost::intrusive::list_base_hook<> all_link = {};
        boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>> bucket_link = {};

        node(const value_type& value) noexcept(std::is_nothrow_copy_constructible_v<value_type>)
            : value_type(value)
        {}

        node(value_type&& value) noexcept(std::is_nothrow_move_constructible_v<value_type>)
            : value_type(std::move(value))
        {}

        value_type& value() noexcept {
            return *this;
        }

        const value_type& value() const noexcept {
            return *this;
        }
    };

    using node_list = boost::intrusive::list<node, boost::intrusive::member_hook<node, boost::intrusive::list_base_hook<>, &node::all_link>, boost::intrusive::constant_time_size<true>>;
    using node_bucket_list = boost::intrusive::list<node, boost::intrusive::member_hook<node, boost::intrusive::list_base_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>, &node::bucket_link>, boost::intrusive::constant_time_size<false>>;

    static node& make_node(const value_type& value) {
        auto np = new node(value);
        return *np;
    }

    static node& make_node(value_type&& value) {
        auto np = new node(std::move(value));
        return *np;
    }

    static void dispose_node(node* np) {
        delete np;
    }

public:
    using iterator = typename node_list::iterator;
    using const_iterator = typename node_list::const_iterator;

    // a specialization of node handle representing a container node
    class node_type {
    public:
        using key_type = Key;
        using mapped_type = T;
        using allocator_type = Allocator;
    
    private:
        std::optional<value_type> _value = {};

    public:
        constexpr node_type() = default;
        node_type(node_type&& o) = default;

        explicit node_type(const value_type& v) noexcept(std::is_nothrow_copy_constructible_v<value_type>)
            : _value(v)
        {}

        explicit node_type(value_type&& v) noexcept(std::is_nothrow_move_constructible_v<value_type>)
            : _value(std::move(v))
        {}

        node_type& operator=(node_type&& o) = default;

        [[nodiscard]] bool empty() const noexcept {
            return bool(_value);
        }

        explicit operator bool() const noexcept {
            return empty();
        }

        key_type& key() const noexcept {
            return _value->first;
        }

        mapped_type& mapped() const noexcept {
            return _value->second;
        }

        void swap(node_type& o) noexcept(std::is_nothrow_move_constructible_v<value_type>) {
            std::swap(_value, o._value);
        }
    };

    struct insert_return_type {
        iterator position;
        bool inserted;
        node_type node;
    };

private:
    node_list _all;
    chunked_vector<node_bucket_list> _buckets;
    ssize_t _bucket_bits = -1;
    size_t _bucket_mask = 0;
    float _max_load_factor = 2.0;

public:
    ~chunked_unordered_map() {
        clear();
    }

    iterator begin() noexcept {
        return _all.begin();
    }

    iterator end() noexcept {
        return _all.end();
    }

    const_iterator begin() const noexcept {
        return cbegin();
    }

    const_iterator end() const noexcept {
        return cend();
    }

    const_iterator cbegin() const noexcept {
        return _all.cbegin();
    }

    const_iterator cend() const noexcept {
        return _all.cend();
    }

    bool empty() const noexcept {
        return _all.empty();
    }

    auto size() const noexcept {
        return _all.size();
    }

    auto max_size() const noexcept {
        std::numeric_limits<size_t>::max();
    }

    size_type bucket_size( size_type n ) const {
        return _buckets[n].size();
    }

    size_type bucket_count() const noexcept {
        return _buckets.size();
    }

    size_type max_bucket_count() const noexcept {
        std::numeric_limits<size_t>::max();
    }

    float load_factor() const noexcept {
        return (float)size() / bucket_count();
    }

    float max_load_factor() const noexcept {
        return _max_load_factor;
    }

    void max_load_factor(float mlf) noexcept {
        _max_load_factor = mlf;
    }

    void clear() noexcept {
        _all.clear_and_dispose(dispose_node);
    }

    bool contains(const Key& key) {
        if (!empty()) {
            auto& bucket = get_bucket(key);
            for (auto it = bucket.begin(); it != bucket.end(); it++) {
                if (it->first == key) {
                    return true;
                }
            }
        }
        return false;
    }

    template <class K>
    iterator find(const K& key) {
        if (!empty()) {
            auto& bucket = get_bucket(key);
            for (auto it = bucket.begin(); it != bucket.end(); it++) {
                if (it->first == key) {
                    return _all.iterator_to(*it);
                }
            }
        }
        return end();
    }

    template <class K>
    const_iterator find(const K& key) const {
        if (!empty()) {
            auto& bucket = get_bucket(key);
            for (auto it = bucket.cbegin(); it != bucket.cend(); it++) {
                if (it->first == key) {
                    return _all.iterator_to(*it);
                }
            }
        }
        return cend();
    }

    std::pair<iterator, bool> insert(const value_type& value) {
        const auto& key = value.first;
        if (!empty()) {
            auto& bucket = get_bucket(key);
            for (auto it = bucket.begin(); it != bucket.end(); it++) {
                if (it->first == key) {
                    return {_all.iterator_to(*it), false};
                }
            }
        }
        auto& n = make_node(value);
        make_room(size() + 1);
        _all.push_back(n);
        auto it = _all.end();
        --it;
        get_bucket(key).push_back(n);
        return {std::move(it), true};
    }

    std::pair<iterator, bool> insert(value_type&& value) {
        const auto& key = value.first;
        if (!empty()) {
            auto& bucket = get_bucket(key);
            for (auto it = bucket.begin(); it != bucket.end(); it++) {
                if (it->first == key) {
                    return {_all.iterator_to(*it), false};
                }
            }
        }
        auto& n = make_node(std::move(value));
        make_room(size() + 1);
        _all.push_back(n);
        auto it = _all.end();
        --it;
        get_bucket(key).push_back(n);
        return {std::move(it), true};
    }

    template <typename... Args>
    std::pair<iterator, bool> try_emplace(const Key& key, Args... args) {
        if (!empty()) {
            auto& bucket = get_bucket(key);
            for (auto it = bucket.begin(); it != bucket.end(); it++) {
                if (it->first == key) {
                    return {_all.iterator_to(*it), false};
                }
            }
        }
        auto& n = make_node(value_type(std::piecewise_construct,
                std::forward_as_tuple(key),
                std::forward_as_tuple(std::forward<Args>(args)...)));
        make_room(size() + 1);
        _all.push_back(n);
        auto it = _all.end();
        --it;
        get_bucket(key).push_back(n);
        return {std::move(it), true};
    }

    template <typename... Args>
    std::pair<iterator, bool> try_emplace(Key&& key, Args... args) {
        if (!empty()) {
            auto& bucket = get_bucket(key);
            for (auto it = bucket.begin(); it != bucket.end(); it++) {
                if (it->first == key) {
                    return {_all.iterator_to(*it), false};
                }
            }
        }
        auto& n = make_node(value_type(std::piecewise_construct,
                std::forward_as_tuple(std::move(key)),
                std::forward_as_tuple(std::forward<Args>(args)...)));
        make_room(size() + 1);
        _all.push_back(n);
        auto it = _all.end();
        --it;
        get_bucket(key).push_back(n);
        return {std::move(it), true};
    }

    template <typename... Args>
    std::pair<iterator, bool> emplace(const Key& key, Args... args) {
        return try_emplace(key, std::forward<Args>(args)...);
    }

    template <typename... Args>
    std::pair<iterator, bool> emplace(Key&& key, Args... args) {
        return try_emplace(std::forward<Key>(key), std::forward<Args>(args)...);
    }

    iterator erase(iterator pos) {
        auto ret = _all.erase_and_dispose(pos, dispose_node);
        shrink_to_fit(_all.size());
        return ret;
    }

    iterator erase(const_iterator pos) {
        auto ret = _all.erase_and_dispose(pos, dispose_node);
        shrink_to_fit(_all.size());
        return ret;
    }

    iterator erase(const_iterator first, const_iterator last) {
        auto ret = _all.erase_and_dispose(first, last, dispose_node);
        shrink_to_fit(_all.size());
        return ret;
    }

    size_type erase(const Key& key) {
        const auto it = find(key);
        if (it != cend()) {
            erase(it);
            return 1;
        }
        return 0;
    }

    template <typename Pred>
    requires std::is_invocable_r_v<bool, Pred, const value_type&>
    size_type
    erase_if(Pred pred) {
        auto old_size = size();
        for (auto it = _all.begin(); it != _all.end(); ) {
            if (pred(*it)) {
                it = _all.erase_and_dispose(it, dispose_node);
            } else {
                ++it;
            }
        }
        auto new_size = size();
        auto ret = old_size - new_size;
        if (ret) {
            shrink_to_fit(new_size);
        }
        return ret;
    }

private:
    static size_t hash(const Key& key, size_t bucket_mask) noexcept {
        return hasher{}(key) & bucket_mask;
    }

    size_t hash(const Key& key) noexcept {
        return hash(key, _bucket_mask);
    }

    node_bucket_list& get_bucket(const Key& key) noexcept {
        return _buckets[hash(key)];
    }

    const node_bucket_list& get_bucket(const Key& key) const noexcept {
        return _buckets[hash(key)];
    }

    void make_room(size_t new_size) {
        auto old_bucket_bits = _bucket_bits;
        size_t old_bucket_size;
        if (old_bucket_bits < 0) {
            _buckets.resize(1);
            _bucket_bits = 0;
            _bucket_mask = 0;
            old_bucket_size = 0;
        } else {
            old_bucket_size = 1 << _bucket_bits;
        }
        while ((double)new_size / (1ULL << _bucket_bits) > _max_load_factor) {
            _bucket_bits++;
            auto new_buckets_size = 1ULL << _bucket_bits;
            _bucket_mask = new_buckets_size - 1;
            _buckets.resize(new_buckets_size);
            for (size_t i = old_bucket_size; i < new_buckets_size; i++) {
                _buckets[i] = {};
            }
            old_bucket_size = new_buckets_size;
        }
        if (old_bucket_bits >= 0) {
            rehash(old_bucket_bits, _bucket_bits);
        }
    }

    void shrink_to_fit(size_t new_size) {
        if (!new_size) {
            _bucket_bits = -1;
            _bucket_mask = 0;
            return;
        }
        auto old_bucket_bits = _bucket_bits;
        while ((1ULL << _bucket_bits) / (double)new_size > _max_load_factor) {
            _bucket_bits--;
            auto new_buckets_size = 1ULL << _bucket_bits;
            _bucket_mask = new_buckets_size - 1;
            _buckets.resize(new_buckets_size);
        }
        if (old_bucket_bits >= 0) {
            rehash(old_bucket_bits, _bucket_bits);
        }
    }

    void rehash(size_t old_bits, size_t new_bits) {
        size_t old_mask = (1 << old_bits) - 1;
        size_t new_mask = (1 << new_bits) - 1;
        for (auto& n : _all) {
            auto old_hash = hash(n.first, old_mask);
            auto new_hash = hash(n.first, new_mask);
            if (old_hash == new_hash) {
                continue;
            }
            n.bucket_link.unlink();
            _buckets[new_hash].push_back(n);
        }
    }
};

} // namespace utils

namespace std {

template<typename Key, class T, size_t max_contiguous_allocation, typename Hash, typename KeyEqual, typename Allocator, typename Pred>
requires std::is_invocable_r_v<bool, Pred, const typename utils::chunked_unordered_map<Key, T, max_contiguous_allocation, Hash, KeyEqual, Allocator>::value_type&>
typename utils::chunked_unordered_map<Key, T, max_contiguous_allocation, Hash, KeyEqual, Allocator>::size_type
erase_if(utils::chunked_unordered_map<Key, T, max_contiguous_allocation, Hash, KeyEqual, Allocator>& m, Pred&& pred) {
    return m.erase_if(std::forward<Pred>(pred));
}

} // namespace std