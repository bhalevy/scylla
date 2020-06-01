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

void metadata_collector::do_update_min_max_components(const clustering_key_prefix& key) {
    auto clustering_key_size = _schema.clustering_key_size();
    auto& types = _schema.clustering_key_type()->types();
    assert(types.size() == clustering_key_size);

    auto& min_seen = min_column_names();
    auto& max_seen = max_column_names();

    // Set initial min==max values
    if (!min_seen.size() || !max_seen.size()) {
        min_seen.resize(clustering_key_size);
        max_seen.resize(clustering_key_size);
        size_t i = 0;
        for (auto& value : key.components()) {
            min_seen[i] = bytes(value.data(), value.size());
            max_seen[i] = bytes(value.data(), value.size());
            i++;
        }
        assert(i == clustering_key_size);
        return;
    }

    size_t i = 0;
    int cmp = 0;
    size_t from = 0;

    // compare key against min_seen
    for (auto& value : key.components()) {
        auto& type = types[i];
        if (!cmp) {
            cmp = type->compare(value, min_seen[i].value());
            if (cmp == 0) {
                i++;
                continue;
            } if (cmp > 0) {
                from = i;
                break;
            }
        }
        min_seen[i] = bytes(value.data(), value.size());
        i++;
    }

    // we're done if key <= min_seen
    if (cmp <= 0) {
        return;
    }

    i = 0;
    cmp = 0;

    // compare key against max_seen
    for (auto& value : key.components()) {
        // key == min_seen prefix up to `from`
        // so it can't be > max_seen
        if (i < from) {
            continue;
        }
        auto& type = types[i];
        if (!cmp) {
            cmp = type->compare(value, max_seen[i].value());
            if (cmp == 0) {
                i++;
                continue;
            } if (cmp < 0) {
                break;
            }
        }
        max_seen[i] = bytes(value.data(), value.size());
        i++;
    }
}

} // namespace sstables
