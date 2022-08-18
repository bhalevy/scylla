/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */
/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */
#pragma once
#include "utils/UUID_gen.hh"
#include "frozen_mutation.hh"

namespace service {

namespace paxos {

struct ballot_id : public utils::tagged_uuid<ballot_id> {
    ballot_id() = default;
    ballot_id(const utils::UUID& uuid) noexcept : utils::tagged_uuid<ballot_id>(uuid) { }

    static ballot_id create_min_time_id() noexcept {
        return ballot_id{utils::UUID_gen::min_time_UUID()};
    }

    int64_t timestamp() const noexcept {
        return uuid().timestamp();
    }

    int64_t as_micros_timestamp() const noexcept {
        return utils::UUID_gen::micros_timestamp(uuid());
    }

    std::chrono::milliseconds as_unix_timestamp() const noexcept {
        return utils::UUID_gen::unix_timestamp(uuid());
    }

    std::chrono::seconds as_unix_timestamp_in_sec() const noexcept {
        return utils::UUID_gen::unix_timestamp_in_sec(uuid());
    }
};

// Proposal represents replica's value associated with a given ballot. The origin uses the term
// "commit" for this object, however, Scylla follows the terminology as set by Paxos Made Simple
// paper.
// Each replica persists the proposals it receives in the system.paxos table. A proposal may be
// new, accepted by a replica, or accepted by a majority. When a proposal is accepted by majority it
// is considered "chosen" by Paxos, and we call such a proposal "decision". A decision is
// saved in the paxos table in an own column and applied to the base table during "learn" phase of
// the protocol. After a decision is applied it is considered "committed".
class proposal {
public:
    // The ballot for the update.
    ballot_id ballot;
    // The mutation representing the update that is being applied.
    frozen_mutation update;

    proposal(ballot_id ballot_arg, frozen_mutation update_arg)
        : ballot(ballot_arg)
        , update(std::move(update_arg)) {}
};

// Proposals are ordered by their ballot's timestamp.
// A proposer uses it to find the newest proposal accepted
// by some replica among the responses to its own one.
inline bool operator<(const proposal& lhs, const proposal& rhs) {
    return lhs.ballot.timestamp() < rhs.ballot.timestamp();
}

inline bool operator>(const proposal& lhs, const proposal& rhs) {
    return lhs.ballot.timestamp() > rhs.ballot.timestamp();
}

// Used for logging and debugging.
std::ostream& operator<<(std::ostream& os, const proposal& proposal);

} // end of namespace "paxos"
} // end of namespace "service"
