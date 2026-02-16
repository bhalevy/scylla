/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/on_internal_error.hh>

#include "mutation_writer/feed_writers.hh"
#include "dht/token.hh"

#include "mutation_writer/vnode_based_splitting_writer.hh"

static logging::logger slogger("vnode_based_splitting_mutation_writer");

namespace mutation_writer {

class vnode_based_splitting_mutation_writer {
    schema_ptr _schema;
    reader_permit _permit;
    mutation_reader_consumer _consumer;
    owned_ranges_ptr _owned_ranges;
    std::optional<bucket_writer> _writer;
    std::optional<dht::token_range_vector::const_iterator> _current_token_range;

public:
    vnode_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, mutation_reader_consumer consumer, owned_ranges_ptr owned_ranges)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _owned_ranges(std::move(owned_ranges))
    {
        if (!_owned_ranges || _owned_ranges->empty()) {
            on_internal_error(slogger, "Owned ranges cannot be empty");
        }
    }

    future<> consume(partition_start&& ps) {
        auto token = ps.key().token();
        bool advance = false;
        if (_current_token_range) {
            auto& token_range = *_current_token_range.value();
            if ((advance = token_range.after(token, dht::token_comparator()))) {
                slogger.info("Token {} is after current range {}: advancing to the next range", token, token_range);
            }
        } else {
            advance = true;
        }
        if (advance) [[unlikely]] {
            if (_writer) {
                _writer->consume_end_of_stream();
                co_await _writer->close();
                _writer.reset();
            }
            do {
                if (_current_token_range) {
                    ++*_current_token_range;
                } else {
                    _current_token_range.emplace(_owned_ranges->begin());
                }
                if (_current_token_range == _owned_ranges->end()) {
                    on_internal_error(slogger, format("Token {} is outside of owned ranges", token));
                }
            } while (!(*_current_token_range)->contains(token, dht::token_comparator()));
            _writer.emplace(_schema, _permit, _consumer);
        }
        co_await _writer->consume(mutation_fragment_v2(*_schema, _permit, std::move(ps)));
    }

    future<> consume(static_row&& sr) {
        return _writer->consume(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return _writer->consume(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone_change&& rt) {
        return _writer->consume(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return _writer->consume(mutation_fragment_v2(*_schema, _permit, std::move(pe)));
    }

    void consume_end_of_stream() {
        if (_writer) {
            _writer->consume_end_of_stream();
        }
    }
    void abort(std::exception_ptr ep) {
        if (_writer) {
            _writer->abort(ep);
        }
    }
    future<> close() noexcept {
        return _writer ? _writer->close() : make_ready_future<>();
    }
};

future<> segregate_by_vnode(mutation_reader producer, mutation_reader_consumer consumer, compaction::owned_ranges_ptr owned_ranges) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(
        std::move(producer),
        vnode_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer), std::move(owned_ranges)));
}

} // namespace mutation_writer
