/*
 * Copyright 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/tagged_integral.hh"

namespace utils {

using generation_type = tagged_integral<struct generation_type_tag, int>;

generation_type get_generation_number();

}
