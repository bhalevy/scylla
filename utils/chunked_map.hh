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

namespace internal {

namespace bi = boost::intrusive;

template<typename Key, typename T,
    typename ValueType = std::pair<const Key, T>>
struct chunked_unordered_map_node : public ValueType {
    using value_type = ValueType;

    using node_list_hook = bi::list_base_hook<>;
    using node_bucket_list_hook = bi::list_base_hook<bi::link_mode<bi::auto_unlink>>;

    node_list_hook all_link = {};
    node_bucket_list_hook bucket_link = {};

    using node_list = bi::list<chunked_unordered_map_node,
            bi::member_hook<chunked_unordered_map_node, node_list_hook, &chunked_unordered_map_node::all_link>,
            bi::constant_time_size<true>>;

    using node_bucket_list = bi::list<chunked_unordered_map_node,
            bi::member_hook<chunked_unordered_map_node, node_bucket_list_hook, &chunked_unordered_map_node::bucket_link>,
            bi::constant_time_size<false>>;

    chunked_unordered_map_node() = delete;

    explicit chunked_unordered_map_node(const value_type& value) noexcept(std::is_nothrow_copy_constructible_v<value_type>)
        : value_type(value)
    {}

    explicit chunked_unordered_map_node(value_type&& value) noexcept(std::is_nothrow_move_constructible_v<value_type>)
        : value_type(std::move(value))
    {}

    chunked_unordered_map_node& operator=(const value_type&) = delete;
    chunked_unordered_map_node& operator=(value_type&&) = delete;

    value_type& value() noexcept {
        return *this;
    }

    const value_type& value() const noexcept {
        return *this;
    }
};

} // namespace internal

template<typename Key, class T, size_t max_contiguous_allocation = 128*1024,
        typename Hash = std::hash<Key>,
        typename KeyEqual = std::equal_to<Key>,
        typename Allocator = std::allocator<internal::chunked_unordered_map_node<Key, T>>
>
class chunked_unordered_map {
    using value_node_type = internal::chunked_unordered_map_node<Key, T>;
    using node_list = typename value_node_type::node_list;
    using node_bucket_list = typename value_node_type::node_bucket_list;

public:
    using key_type = Key;
    using mapped_type = T;
    using size_type = size_t;
    using difference_type = std::ptrdiff_t;
    using hasher = Hash;
    using key_equal = KeyEqual;
    using allocator_type = Allocator;
    using value_type = typename value_node_type::value_type;
    using reference = value_type&;
    using const_reference = const value_type&;
    using pointer = typename std::allocator_traits<Allocator>::pointer;
    using const_pointer = typename std::allocator_traits<Allocator>::const_pointer;

private:
    static value_node_type& make_value_node(const value_type& value, allocator_type& alloc) {
        auto np = alloc.allocate(1);
        return *std::construct_at(np, value);
    }

    static value_node_type& make_value_node(value_type&& value, allocator_type& alloc) {
        auto np = alloc.allocate(1);
        return *std::construct_at(np, std::move(value));
    }

    static void dispose_value_node(value_node_type* np, allocator_type& alloc) {
        std::destroy_at(np);
        alloc.deallocate(np, 1);
    }

    value_node_type& make_value_node(const value_type& value) {
        return make_value_node(value, _alloc);
    }

    value_node_type& make_value_node(value_type&& value) {
        return make_value_node(std::move(value), _alloc);
    }

    void dispose_value_node(value_node_type* np) {
        dispose_value_node(np, _alloc);
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
        std::unique_ptr<value_node_type, std::function<void (value_node_type*)>> _value = {nullptr, [] (value_node_type*) {}};

    public:
        constexpr node_type() = default;
        node_type(node_type&& o) = default;

        explicit node_type(value_node_type* np, allocator_type& alloc) noexcept(std::is_nothrow_move_constructible_v<value_node_type>)
            : _value(np, [&alloc] (value_node_type* np) {
                chunked_unordered_map::dispose_value_node(np, alloc);
            })
        {}

        node_type& operator=(node_type&& o) = default;

        [[nodiscard]] bool empty() const noexcept {
            return !_value;
        }

        explicit operator bool() const noexcept {
            return !empty();
        }

        key_type& key() const noexcept {
            return *const_cast<key_type*>(&_value->first);
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
    Hash _hash = Hash();
    key_equal _key_equal = KeyEqual();
    Allocator _alloc = Allocator();

    node_list _all;
    chunked_vector<node_bucket_list> _buckets;

    size_t _min_bucket_count = 0;
    ssize_t _bucket_bits = -1;
    size_t _bucket_mask = 0;
    float _max_load_factor = 1.0;

public:
    chunked_unordered_map() = default;

    chunked_unordered_map(chunked_unordered_map&& o, const Allocator& alloc) noexcept
        : _hash(std::move(o._hash))
        , _key_equal(std::move(o._key_equal))
        , _alloc(alloc)
        , _all(std::move(o._all))
        , _buckets(std::move(o._buckets))
        , _min_bucket_count(std::exchange(o._min_bucket_count, 0))
        , _bucket_bits(std::exchange(o._bucket_bits, -1))
        , _bucket_mask(std::exchange(o._bucket_mask, 0))
        , _max_load_factor(std::exchange(o._max_load_factor, 1.0))
    {}

    chunked_unordered_map(chunked_unordered_map&& o) noexcept
        : chunked_unordered_map(std::move(o), Allocator())
    {}

    chunked_unordered_map(const chunked_unordered_map& o, const Allocator& alloc)
        : _hash(o._hash)
        , _key_equal(o._key_equal)
        , _alloc(o._alloc)
        , _all()
        , _buckets()
        , _min_bucket_count(o._min_bucket_count)
        , _bucket_bits(o._bucket_bits)
        , _bucket_mask(o._bucket_mask)
        , _max_load_factor(o._max_load_factor)
    {
        _buckets.resize(o._buckets.size());
        for (size_t i = 0; i < o._buckets.size(); i++) {
            auto& b = _buckets[i];
            for (auto it = o._buckets[i].cbegin(); it != o._buckets[i].cend(); it++) {
                auto* np = _alloc.allocate(1);
                _alloc.construct(*np, *it);
                b.push_back(*np);
                _all.push_back(*np);
            }
        }
    }

    chunked_unordered_map(const chunked_unordered_map& o)
        : chunked_unordered_map(o, Allocator())
    {}

    explicit chunked_unordered_map(size_type bucket_count,
            const Hash& hash = Hash(),
            const key_equal& key_equal = KeyEqual(),
            const Allocator& alloc = Allocator())
            : _hash(hash)
            , _key_equal(key_equal)
            , _alloc(alloc)
    {
        if (bucket_count) {
            while ((1ULL << ++_bucket_bits) < bucket_count) {}
            _min_bucket_count = (1ULL << _bucket_bits);
            _bucket_mask = _min_bucket_count - 1;
        }
    }

    explicit chunked_unordered_map(const Allocator& alloc)
            : chunked_unordered_map(0, Hash(), KeyEqual(), alloc)
    {}

    chunked_unordered_map(size_type bucket_count,
            const Allocator& alloc)
            : chunked_unordered_map(bucket_count, Hash(), KeyEqual(), alloc)
    {}

    chunked_unordered_map(size_type bucket_count,
            const Hash& hash,
            const Allocator& alloc)
            : chunked_unordered_map(bucket_count, hash, KeyEqual(), alloc)
    {}

    template <typename InputIt>
    chunked_unordered_map(InputIt first, InputIt last,
            size_type bucket_count = std::numeric_limits<size_type>::max(),
            const Hash& hash = Hash(),
            const key_equal& key_equal = KeyEqual(),
            const Allocator& alloc = Allocator())
            : chunked_unordered_map(bucket_count == std::numeric_limits<size_type>::max() ? std::distance(first, last) : bucket_count,
                    hash, key_equal, alloc)
    {
        for (auto it = first; it != last; ++it) {
            insert(*it);
        }
    }

    template <typename InputIt>
    chunked_unordered_map(InputIt first, InputIt last, size_type bucket_count,
            const Allocator& alloc = Allocator())
            : chunked_unordered_map(first, last, bucket_count, Hash(), KeyEqual(), alloc)
    {}

    template <typename InputIt>
    chunked_unordered_map(InputIt first, InputIt last, size_type bucket_count,
            const Hash& hash,
            const Allocator& alloc)
            : chunked_unordered_map(first, last, bucket_count, hash, KeyEqual(), alloc)
    {}

    chunked_unordered_map(std::initializer_list<value_type> init,
            size_type bucket_count = std::numeric_limits<size_type>::max(),
            const Hash& hash = Hash(),
            const key_equal& key_equal = KeyEqual(),
            const Allocator& alloc = Allocator())
            : chunked_unordered_map(init.begin(), init.end(), bucket_count, hash, key_equal, alloc)
    {}

    chunked_unordered_map(std::initializer_list<value_type> init, size_type bucket_count,
            const Allocator& alloc)
            : chunked_unordered_map(init.begin(), init.end(), bucket_count, Hash(), KeyEqual(), alloc)
    {}

    chunked_unordered_map(std::initializer_list<value_type> init, size_type bucket_count,
            const Hash& hash,
            const Allocator& alloc)
            : chunked_unordered_map(init.begin(), init.end(), bucket_count, hash, KeyEqual(), alloc)
    {}

    ~chunked_unordered_map() {
        clear();
    }

    chunked_unordered_map& operator=(chunked_unordered_map&& o) noexcept {
        clear();
    }

    chunked_unordered_map& operator=(const chunked_unordered_map& o) {
        clear();
    }

    void swap(chunked_unordered_map& o) noexcept {
        std::swap(_all, o._all);
        std::swap(_buckets, o._buckets);
        std::swap(_bucket_bits, o._bucket_bits);
        std::swap(_bucket_mask, o._bucket_mask);
        std::swap(_max_load_factor, o._max_load_factor);
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
        _all.clear_and_dispose([this] (value_node_type* np) {
            dispose_value_node(np);
        });
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
        auto& n = make_value_node(value);
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
        auto& n = make_value_node(std::move(value));
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
        auto& n = make_value_node(value_type(std::piecewise_construct,
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
        auto& n = make_value_node(value_type(std::piecewise_construct,
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

    node_type extract(const_iterator pos) {
        value_node_type* np = const_cast<value_node_type*>(&*pos);
        auto ret = node_type(np, _alloc);
        _all.erase(pos);
        return ret;
    }

    node_type extract(const Key& key) {
        auto it = find(key);
        if (it != end()) {
            return extract(it);
        }
        return node_type();
    }

    iterator erase(iterator pos) {
        auto ret = _all.erase_and_dispose(pos, [this] (value_node_type* np) {
            dispose_value_node(np);
        });
        shrink_to_fit(_all.size());
        return ret;
    }

    iterator erase(const_iterator pos) {
        auto ret = _all.erase_and_dispose(pos, [this] (value_node_type* np) {
            dispose_value_node(np);
        });
        shrink_to_fit(_all.size());
        return ret;
    }

    iterator erase(const_iterator first, const_iterator last) {
        auto ret = _all.erase_and_dispose(first, last, [this] (value_node_type* np) {
            dispose_value_node(np);
        });
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
                it = _all.erase_and_dispose(it, [this] (value_node_type* np) {
                    dispose_value_node(np);
                });
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
    size_t hash(const Key& key, size_t mask) const noexcept {
        return _hash(key) & mask;
    }

    size_t hash(const Key& key) const noexcept {
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

template<typename Key, class T, size_t max_contiguous_allocation, typename Hash, typename KeyEqual, typename Allocator>
void
swap(utils::chunked_unordered_map<Key, T, max_contiguous_allocation, Hash, KeyEqual, Allocator>& lhs, utils::chunked_unordered_map<Key, T, max_contiguous_allocation, Hash, KeyEqual, Allocator>& rhs) {
    lhs.swap(rhs);
}

} // namespace std