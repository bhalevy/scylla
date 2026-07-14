/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "utils/UUID.hh"

namespace sstables {

using run_id = utils::tagged_uuid<struct run_id_tag>;
using integrity_check = bool_class<class integrity_check_tag>;

enum class update_sstable_id {
    no,
    yes,
    // FIXME: `maybe` lets the lower level decide automatically based on the sstable storage type.
};

} // namespace sstables
