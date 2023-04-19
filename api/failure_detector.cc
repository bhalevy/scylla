/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "failure_detector.hh"
#include "api/api-doc/failure_detector.json.hh"
#include "gms/application_state.hh"
#include "gms/gossiper.hh"
#include "utils/bounded_stats_deque.hh"
#include "log.hh"

extern logging::logger apilog;

namespace gms {

class arrival_window {
public:
    using clk = seastar::lowres_system_clock;
private:
    clk::time_point _tlast{clk::time_point::min()};
    utils::bounded_stats_deque _arrival_intervals;
    std::chrono::milliseconds _initial;
    std::chrono::milliseconds _max_interval;
    std::chrono::milliseconds _min_interval;

    // this is useless except to provide backwards compatibility in phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users who have
    // already tuned their phi_convict_threshold for their own environments won't need to
    // change.
    static constexpr double PHI_FACTOR{M_LOG10El};

public:
    arrival_window(int size, std::chrono::milliseconds initial,
            std::chrono::milliseconds max_interval, std::chrono::milliseconds min_interval)
        : _arrival_intervals(size)
        , _initial(initial)
        , _max_interval(max_interval)
        , _min_interval(min_interval) {
    }

    void add(clk::time_point value, const gms::inet_address& ep);

    double mean() const;

    // see CASSANDRA-2597 for an explanation of the math at work here.
    double phi(clk::time_point tnow);

    size_t size() { return _arrival_intervals.size(); }

    clk::time_point last_update() const { return _tlast; }

    friend std::ostream& operator<<(std::ostream& os, const arrival_window& w);

};

void arrival_window::add(clk::time_point value, const gms::inet_address& ep) {
    if (_tlast > clk::time_point::min()) {
        auto inter_arrival_time = value - _tlast;
        if (inter_arrival_time <= _max_interval && inter_arrival_time >= _min_interval) {
            _arrival_intervals.add(inter_arrival_time.count());
        } else  {
            apilog.debug("failure_detector: Ignoring interval time of {} for {}, mean={}, size={}", inter_arrival_time.count(), ep, mean(), size());
        }
    } else {
        // We use a very large initial interval since the "right" average depends on the cluster size
        // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
        // than low (false positives, which cause "flapping").
        _arrival_intervals.add(_initial.count());
    }
    _tlast = value;
}

double arrival_window::mean() const {
    return _arrival_intervals.mean();
}

double arrival_window::phi(clk::time_point tnow) {
    assert(_arrival_intervals.size() > 0 && _tlast > clk::time_point::min()); // should not be called before any samples arrive
    auto t = (tnow - _tlast).count();
    auto m = mean();
    double phi = t / m;
    apilog.debug("failure_detector: now={}, tlast={}, t={}, mean={}, phi={}",
        tnow.time_since_epoch().count(), _tlast.time_since_epoch().count(), t, m, phi);
    return phi;
}

std::ostream& operator<<(std::ostream& os, const arrival_window& w) {
    for (auto& x : w._arrival_intervals.deque()) {
        os << x << " ";
    }
    return os;
}

} // namespace gms

namespace api {
using namespace seastar::httpd;

namespace fd = httpd::failure_detector_json;

void set_failure_detector(http_context& ctx, routes& r, gms::gossiper& g) {
    fd::get_all_endpoint_states.set(r, [&g](std::unique_ptr<request> req) {
        std::vector<fd::endpoint_state> res;
        for (auto i : g.get_endpoint_states()) {
            fd::endpoint_state val;
            val.addrs = fmt::to_string(i.first);
            val.is_alive = i.second.is_alive();
            val.generation = i.second.get_heart_beat_state().get_generation();
            val.version = i.second.get_heart_beat_state().get_heart_beat_version();
            val.update_time = i.second.get_update_timestamp().time_since_epoch().count();
            for (auto a : i.second.get_application_state_map()) {
                fd::version_value version_val;
                // We return the enum index and not it's name to stay compatible to origin
                // method that the state index are static but the name can be changed.
                version_val.application_state = static_cast<std::underlying_type<gms::application_state>::type>(a.first);
                version_val.value = a.second.value;
                version_val.version = a.second.version;
                val.application_state.push(version_val);
            }
            res.push_back(val);
        }
        return make_ready_future<json::json_return_type>(res);
    });

    fd::get_up_endpoint_count.set(r, [&g](std::unique_ptr<request> req) {
        int res = g.get_up_endpoint_count();
        return make_ready_future<json::json_return_type>(res);
    });

    fd::get_down_endpoint_count.set(r, [&g](std::unique_ptr<request> req) {
        int res = g.get_down_endpoint_count();
        return make_ready_future<json::json_return_type>(res);
    });

    fd::get_phi_convict_threshold.set(r, [] (std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(8);
    });

    fd::get_simple_states.set(r, [&g] (std::unique_ptr<request> req) {
        std::map<sstring, sstring> nodes_status;
        for (auto& entry : g.get_endpoint_states()) {
            nodes_status.emplace(entry.first.to_sstring(), entry.second.is_alive() ? "UP" : "DOWN");
        }
        return make_ready_future<json::json_return_type>(map_to_key_value<fd::mapper>(nodes_status));
    });

    fd::set_phi_convict_threshold.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        std::ignore = atof(req->get_query_param("phi").c_str());
        return make_ready_future<json::json_return_type>("");
    });

    fd::get_endpoint_state.set(r, [&g] (std::unique_ptr<request> req) {
        auto* state = g.get_endpoint_state_for_endpoint_ptr(gms::inet_address(req->param["addr"]));
        if (!state) {
            return make_ready_future<json::json_return_type>(format("unknown endpoint {}", req->param["addr"]));
        }
        std::stringstream ss;
        g.append_endpoint_state(ss, *state);
        return make_ready_future<json::json_return_type>(sstring(ss.str()));
    });

    fd::get_endpoint_phi_values.set(r, [](std::unique_ptr<request> req) {
        std::map<gms::inet_address, gms::arrival_window> map;
        std::vector<fd::endpoint_phi_value> res;
        auto now = gms::arrival_window::clk::now();
        for (auto& p : map) {
            fd::endpoint_phi_value val;
            val.endpoint = p.first.to_sstring();
            val.phi = p.second.phi(now);
            res.emplace_back(std::move(val));
        }
        return make_ready_future<json::json_return_type>(res);
    });
}

}

