/*
 * Copyright 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/tagged_integral.hh"

namespace utils {

template<typename Tag, typename ValueType>
struct tagged_integral final {
    ValueType value();
};

} // namespace utils
