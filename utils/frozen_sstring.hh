/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

using namespace seastar;

namespace utils {

template <typename tag, typename char_type, typename Size, Size max_size, bool NulTerminate>
class basic_frozen_sstring {
    using basic_sstring =  basic_sstring<char_type, Size, max_size, NulTerminate>;
    using basic_string_view = std::basic_string_view<char_type>;
    using basic_string = std::basic_string<char_type>;

    basic_sstring _str;
    size_t _hash;

public:
    using value_type = basic_sstring::value_type;
    using traits_type = basic_sstring::traits_type;
    using allocator_type = basic_sstring::allocator_type;
    using reference = basic_sstring::const_reference;
    using const_reference = basic_sstring::const_reference;
    using pointer = basic_sstring::const_pointer;
    using const_pointer = basic_sstring::const_pointer;
    using iterator = basic_sstring::const_iterator;
    using const_iterator = basic_sstring::const_iterator;
    using difference_type = basic_sstring::difference_type;
    using size_type = basic_sstring::size_type;

    static constexpr size_type npos = basic_sstring::npos;

    basic_frozen_sstring() noexcept : _str(), _hash(0) {}
    basic_frozen_sstring(const basic_frozen_sstring& o) : _str(o._str) , _hash(o._hash) {}
    basic_frozen_sstring(basic_frozen_sstring&& o) noexcept : _str(std::move(o._str)), _hash(std::exchange(o._hash, 0)) {}

    explicit basic_frozen_sstring(const char_type* s) noexcept : _str(s), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const char_type* s, size_type size) noexcept : _str(s, size), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const basic_string_view& x) : _str(x), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const basic_string& x) : _str(x), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(const basic_sstring& x) : _str(x), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(basic_string&& x) : _str(std::move(x)), _hash(std::hash<basic_sstring>()(_str)) {}
    explicit basic_frozen_sstring(basic_sstring&& x) : _str(std::move(x)), _hash(std::hash<basic_sstring>()(_str)) {}

    basic_frozen_sstring& operator=(const basic_frozen_sstring& x) {
        if (this != &x) {
            _str = x._str;
            _hash = x._hash;
        }
        return *this;
    }
    basic_frozen_sstring& operator=(basic_frozen_sstring&& x) noexcept {
        if (this != &x) {
            _str = std::move(x._str);
            _hash = std::exchange(x._hash, 0);
        }
        return *this;
    }

    bool operator==(const basic_frozen_sstring& x) const noexcept {
        return hash() == x.hash() && str() == x.str();
    }

    constexpr std::strong_ordering operator<=>(const basic_frozen_sstring& x) const noexcept {
        return str() <=> x.str();
    }

    const char_type& operator[](size_type pos) const noexcept {
        return str()[pos];
    }

    const basic_sstring& str() const noexcept { return _str; }
    const_pointer c_str() const noexcept { return str().c_str(); }
    const_pointer data() const noexcept { return str().data(); }
    size_t hash() const noexcept { return _hash; }

    bool empty() const noexcept { return str().empty(); }
    size_t size() const noexcept { return str().size(); }

    const_iterator begin() const noexcept { return str().begin(); }
    const_iterator end() const noexcept { return str().end(); }
    const_iterator cbegin() const noexcept { return str().cbegin(); }
    const_iterator cend() const noexcept { return str().cend(); }

    const_reference front() const noexcept { return str().front(); }
    const_reference back() const noexcept { return str().back(); }

    const char_type& at(size_t pos) const { return str().at(pos); }

    constexpr size_t find(const char_type* s, size_t pos = 0) const noexcept {
        return str().find(s, pos);
    }
    size_t find(char_type t, size_t pos = 0) const noexcept {
        return str().find(t, pos);
    }
    size_t find(const char_type* c_str, size_t pos, size_t len2) const noexcept {
        return str().find(c_str, pos, len2);
    }
    size_t find(const basic_sstring& s, size_t pos = 0) const noexcept {
        return find(s.c_str(), pos, s.size());
    }
    size_t find(const basic_frozen_sstring& s, size_t pos = 0) const noexcept {
        return find(s.c_str(), pos, s.size());
    }
    template<class StringViewLike,
             std::enable_if_t<std::is_convertible_v<StringViewLike,
                                                    std::basic_string_view<char_type, traits_type>>,
                              int> = 0>
    size_t find(const StringViewLike& sv_like, size_type pos = 0) const noexcept {
        return str().find(sv_like, pos);
    }
    size_t find_last_of(char_type c, size_t pos = npos) const noexcept {
        return str().find_last_of(c, pos);
    }

    basic_frozen_sstring substr(size_t from, size_t len = npos) const {
        return str().substr(from, len);
    }

    constexpr bool starts_with(std::basic_string_view<char_type, traits_type> sv) const noexcept {
        return str().starts_with(sv);
    }
    constexpr bool starts_with(char_type c) const noexcept {
        return str().starts_with(c);
    }
    constexpr bool starts_with(const char_type* s) const noexcept {
        return str().starts_with(s);
    }
    constexpr bool ends_with(std::basic_string_view<char_type, traits_type> sv) const noexcept {
        return str().ends_with(sv);
    }
    constexpr bool ends_with(char_type c) const noexcept {
        return str().ends_with(c);
    }
    constexpr bool ends_with(const char_type* s) const noexcept {
        return str().ends_with(s);
    }
    constexpr bool contains(std::basic_string_view<char_type, traits_type> sv) const noexcept {
        return str().contains(sv);
    }
    constexpr bool contains(char_type c) const noexcept {
        return str().contains(c);
    }
    constexpr bool contains(const char_type* s) const noexcept {
        return str().contains(s);
    }

private:
    auto format(fmt::format_context& ctx) const {
        return fmt::formatter<basic_sstring>().format(str(), ctx);
    }

    friend fmt::formatter<basic_frozen_sstring>;
};

using frozen_sstring = basic_frozen_sstring<struct untagged_frozen_sstring, char, uint32_t, 15, true>;

} // namespace utils

template <typename tag, typename char_type, typename size_type, size_type max_size, bool NulTerminate>
struct fmt::formatter<utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>& s, fmt::format_context& ctx) const {
        return s.format(ctx);
    }
};

namespace std {

template <typename tag, typename char_type, typename size_type, size_type max_size, bool NulTerminate>
struct hash<utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>> {
    size_t operator()(const utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>& s) const {
        return s.hash();
    }
};

template <typename tag, typename char_type, typename size_type, size_type max_size, bool NulTerminate>
std::ostream& operator<<(std::ostream& os, const utils::basic_frozen_sstring<tag, char_type, size_type, max_size, NulTerminate>& s) {
    return os << s.str();
}

} // namespace std
