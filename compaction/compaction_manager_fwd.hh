/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

class compaction_manager;
using compaction_manager_opt = const compaction_manager* const;
constexpr compaction_manager_opt compaction_manager_nullopt = nullptr;
