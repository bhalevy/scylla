/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "idl/result.idl.hh"
#include "idl/uuid.idl.hh"

namespace service {
namespace paxos {

class ballot_id final {
    utils::UUID uuid();
};

class proposal {
    service::paxos::ballot_id ballot;
    frozen_mutation update;
};

class promise {
    std::optional<service::paxos::proposal> accepted_proposal;
    std::optional<service::paxos::proposal> most_recent_commit;
    std::optional<std::variant<query::result, query::result_digest>> get_data_or_digest();
};

}
}
