/*
 * Copyright (C) 2019 ScyllaDB
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

#include <seastar/core/print.hh>

#include "position_in_partition.hh"
#include "utils/exceptions.hh"
#include "log.hh"

logging::logger plogger("position_in_partition");

const sstring position_in_partition_tracker::current_position_string() {
    if (is_beginning_of_stream()) {
        return "beginning of stream";
    }
    if (is_partition_start()) {
        return "partition start";
    }
    if (is_static_row()) {
        return "static row";
    }
    if (is_clustering_row()) {
        return "clustering row";
    }
    if (is_partition_end()) {
        return "partition end";
    }
    if (is_end_of_stream()) {
        return "end of stream";
    }
    return "unrecognized position_in_partition";
}

void position_in_partition_tracker::on_partition_start() {
    plogger.trace("partition start");
    if (is_partition_end() || is_beginning_of_stream()) {
        _pos = partition_region::partition_start;
        _in_stream = true;
    } else {
        on_internal_error(plogger, format("New partition must not start after {}", current_position_string()));
    }
}

void position_in_partition_tracker::on_static_row() {
    plogger.trace("static row");
    if (is_partition_start()) {
        _pos = partition_region::static_row;
    } else {
        on_internal_error(plogger, format("Static row must not start after {}", current_position_string()));
    }
}

void position_in_partition_tracker::on_clustering_row() {
    plogger.trace("clustering row");
    if (is_clustering_row()) {
        return;
    } else if (is_partition_start() || is_static_row()) {
        _pos = partition_region::clustered;
    } else {
        on_internal_error(plogger, format("Clustering row must not start after {}", current_position_string()));
    }
}

void position_in_partition_tracker::on_partition_end() {
    plogger.trace("partition end");
    if (in_stream() && !is_partition_end()) {
        _pos = partition_region::partition_end;
    } else {
        on_internal_error(plogger, format("Partition must not end after {}", current_position_string()));
    }
}

void position_in_partition_tracker::on_end_of_stream() {
    plogger.trace("end of stream");
    if (is_partition_end()) {
        _in_stream = false;
    } else {
        on_internal_error(plogger, format("Stream must not end after {}", current_position_string()));
    }
}
