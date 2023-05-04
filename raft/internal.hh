/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <ostream>
#include <functional>
#include "utils/UUID.hh"
#include "utils/tagged_integer.hh"

namespace raft {
namespace internal {

// Unlike utils::tagged_uuid, tagged_id has a non-final idl type.
template<typename Tag>
using tagged_id = utils::tagged_uuid<Tag>;

// Unlike utils::tagged_integer, tagged_uint64 has a non-final idl type.
template<typename Tag>
using tagged_uint64 = utils::tagged_integer<Tag, uint64_t>;

} // end of namespace internal
} // end of namespace raft
