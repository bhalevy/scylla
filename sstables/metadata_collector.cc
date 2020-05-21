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

/*
 * Copyright (C) 2020 ScyllaDB
 */

#include "sstables/metadata_collector.hh"

namespace sstables {

void metadata_collector::do_update_min_max_components(const schema& schema, const clustering_key_prefix& key, bool update_suffix) {
    auto may_grow = [] (std::vector<bytes_opt>& v, size_t target_size) {
        if (target_size > v.size()) {
            v.resize(target_size);
        }
    };

    auto clustering_key_size = schema.clustering_key_size();
    auto& min_seen = min_column_names();
    auto& max_seen = max_column_names();
    may_grow(min_seen, clustering_key_size);
    may_grow(max_seen, clustering_key_size);

    auto& types = schema.clustering_key_type()->types();
    auto i = 0U;
    for (auto& value : key.components()) {
        auto& type = types[i];

        if (!max_seen[i] || (max_seen[i].value().size() && type->compare(value, max_seen[i].value()) > 0)) {
            max_seen[i] = bytes(value.data(), value.size());
        }
        if (!min_seen[i] || (min_seen[i].value().size() && type->compare(value, min_seen[i].value()) < 0)) {
            min_seen[i] = bytes(value.data(), value.size());
        }
        i++;
    }

    // range_tombstones that contain only a prefix of the clustering key components
    // implicitly apply to unbounded ranges of the remaining components.
    // Indicate that by using empty values.
    //
    // This is required for supporting the md sstable format.
    // See https://issues.apache.org/jira/browse/CASSANDRA-14861
    if (update_suffix) {
        while (i < clustering_key_size) {
            min_seen[i] = bytes();
            max_seen[i] = bytes();
        }
    }
}

} // namespace sstables
