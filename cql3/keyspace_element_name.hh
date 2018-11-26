/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "seastarx.hh"

#include <experimental/optional>

namespace cql3 {

/**
 * Base class for the names of the keyspace elements (e.g. table, index ...)
 */
class keyspace_element_name {
    /**
     * The keyspace name as stored internally.
     */
    std::experimental::optional<sstring> _ks_name = std::experimental::nullopt;

public:
    /**
     * Sets the keyspace.
     *
     * @param ks the keyspace name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     */
    void set_keyspace(const sstring& ks, bool keep_case);

    /**
     * Checks if the keyspace is specified.
     * @return <code>true</code> if the keyspace is specified, <code>false</code> otherwise.
     */
    bool has_keyspace() const;

    const sstring& get_keyspace() const;

    virtual sstring to_string() const;

protected:
    /**
     * Converts the specified name into the name used internally.
     *
     * @param name the name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     * @return the name used internally.
     */
    static sstring to_internal_name(sstring name, bool keep_case);
};

}
