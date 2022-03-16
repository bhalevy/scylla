/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation_fragment_v2.hh"

template <typename EndConsumer>
requires requires(EndConsumer& c, mutation_fragment_v2 mf) {
    { c(std::move(mf)) };
}
class upgrading_consumer {
    const schema& _schema;
    reader_permit _permit;
    EndConsumer _end_consumer;

    range_tombstone_change_generator _rt_gen;
    tombstone _current_rt;
    std::optional<position_range> _pr;

    template <typename Frag>
    void do_consume(Frag&& frag) {
        _end_consumer(mutation_fragment_v2(_schema, _permit, std::move(frag)));
    }

public:
    upgrading_consumer(const schema& schema, reader_permit permit, EndConsumer&& end_consumer)
        : _schema(schema), _permit(std::move(permit)), _end_consumer(std::move(end_consumer)), _rt_gen(_schema)
    {}
    void set_position_range(position_range pr) {
        _rt_gen.trim(pr.start());
        _current_rt = {};
        _pr = std::move(pr);
    }
    bool discardable() const {
        return _rt_gen.discardable() && !_current_rt;
    }
    void flush_tombstones(position_in_partition_view pos) {
        _rt_gen.flush(pos, [&] (range_tombstone_change rt) {
            _current_rt = rt.tombstone();
            do_consume(std::move(rt));
        });
    }
    void consume(partition_start mf) {
        _rt_gen.reset();
        _current_rt = {};
        _pr = {};
        do_consume(std::move(mf));
    }
    void consume(static_row mf) {
        do_consume(std::move(mf));
    }
    void consume(clustering_row mf) {
        flush_tombstones(mf.position());
        do_consume(std::move(mf));
    }
    void consume(range_tombstone rt) {
        if (_pr && !rt.trim_front(_schema, _pr->start())) {
            return;
        }
        flush_tombstones(rt.position());
        _rt_gen.consume(std::move(rt));
    }
    void consume(partition_end mf) {
        flush_tombstones(position_in_partition::after_all_clustered_rows());
        if (_current_rt) {
            assert(!_pr);
            do_consume(range_tombstone_change(position_in_partition::after_all_clustered_rows(), {}));
        }
        do_consume(std::move(mf));
    }
    void consume(mutation_fragment&& mf) {
        std::move(mf).consume(*this);
    }
    void on_end_of_stream() {
        if (_pr) {
            flush_tombstones(_pr->end());
            if (_current_rt) {
                do_consume(range_tombstone_change(_pr->end(), {}));
            }
        }
    }
};
