/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <functional>

namespace utils {

template <class T>
class optional_reference {
    std::optional<std::reference_wrapper<T>> _value;
public:
    optional_reference() = default;
    optional_reference(std::nullopt_t noopt) : _value(noopt) {}

    optional_reference(T& t) noexcept : _value(std::make_optional(std::ref(t))) {}

    template <class U>
    requires std::is_const_v<T> && std::same_as<U, std::remove_const<T>>
    optional_reference(U& t) noexcept : _value(std::make_optional(std::cref(t))) {}

    bool has_value() const noexcept {
        return _value.has_value();
    }

    explicit operator bool() const noexcept {
        return has_value();
    }

    T& value() noexcept {
        return _value->get();
    }

    const T& value() const noexcept {
        return _value->get();
    }

    T& operator*() noexcept {
        return value();
    }

    const T& operator*() const noexcept {
        return value();
    }

    T* operator->() noexcept {
        return &value();
    }

    const T* operator->() const noexcept {
        return &value();
    }
};

template <class T>
optional_reference<T> make_optional_reference(T& t) {
    return std::make_optional(std::ref(t));
}

} // namespace utils
