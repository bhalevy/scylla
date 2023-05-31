#pragma once

/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <cmath>

namespace utils {

template <unsigned i>
constexpr double const_log() {
    return std::log(i);
}

template <double x>
constexpr double const_log() {
    return std::log(x);
}

template <unsigned i>
constexpr double inv_const_log() {
    return 1.0 / std::log(i);
}

template <double x>
constexpr double inv_const_log() {
    return 1.0 / std::log(x);
}

template <unsigned base>
inline double log_base(double x) {
    return std::log(x) * inv_const_log<base>();
}

} // namespace utils
