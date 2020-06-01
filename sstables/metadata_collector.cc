/*
 * Copyright (C) 2020 ScyllaDB
 */

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

#include "log.hh"
#include "metadata_collector.hh"

logging::logger mdclogger("metadata_collector");

namespace sstables {

void metadata_collector::convert(disk_array<uint32_t, disk_string<uint16_t>>&to, const bound_view& from) {
    mdclogger.trace("{}: convert size={}", _name, from.prefix().size(_schema));
    if (from.prefix().is_empty()) {
        return;
    }
    for (auto value : from.prefix().components()) {
        to.elements.push_back(disk_string<uint16_t>{bytes(value.data(), value.size())});
    }
}

void metadata_collector::do_update_min_max_components(const clustering_key_prefix& key) {
    const bound_view::tri_compare cmp(_schema);
    bool set_min = false;

    if (cmp(key, _min_bound) < 0) {
        mdclogger.trace("{}: setting min_bound={} size={}", _name, key, key.size(_schema));
        _min_clustering_key = make_lw_shared<clustering_key_prefix>(key);
        _min_bound = bound_view(*_min_clustering_key, bound_kind::incl_start);
        set_min = true;
    }

    if (cmp(key, _max_bound) > 0) {
        mdclogger.trace("{}: setting max_bound={} size={}", _name, key, key.size(_schema));
        _max_clustering_key = set_min ? _min_clustering_key : make_lw_shared<clustering_key_prefix>(key);
        _max_bound = bound_view(*_max_clustering_key, bound_kind::incl_end);
    }
}

} // namespace sstables
