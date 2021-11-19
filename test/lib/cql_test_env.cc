/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/algorithm/transform.hpp>
#include <iterator>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include "replica/database_fwd.hh"
#include "sstables/sstables.hh"
#include <seastar/core/do_with.hh>
#include "test/lib/cql_test_env.hh"
#include "cdc/generation_service.hh"
#include "cql3/functions/functions.hh"
#include "cql3/query_processor.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/batch_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "cql3/cql_config.hh"
#include <seastar/core/distributed.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include "utils/UUID_gen.hh"
#include "service/migration_manager.hh"
#include "service/tablet_allocator.hh"
#include "compaction/compaction_manager.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_address_map.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "service/forward_service.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "auth/service.hh"
#include "auth/common.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "schema/schema_builder.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"
#include "unit_test_service_levels_accessor.hh"
#include "db/view/view_builder.hh"
#include "db/view/node_view_update_backlog.hh"
#include "db/view/view_update_generator.hh"
#include "replica/distributed_loader.hh"
// TODO: remove (#293)
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/sstables-format-selector.hh"
#include "repair/row_level.hh"
#include "utils/cross-shard-barrier.hh"
#include "streaming/stream_manager.hh"
#include "debug.hh"
#include "db/schema_tables.hh"
#include "db/virtual_tables.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/raft/raft_group0.hh"
#include "sstables/sstables_manager.hh"
#include "init.hh"
#include "utils/fb_utilities.hh"
#include "service/service_ctl.hh"

#include <sys/time.h>
#include <sys/resource.h>

using namespace std::chrono_literals;

namespace {


} // anonymous namespace

future<scheduling_groups> get_scheduling_groups() {
    static std::optional<scheduling_groups> _scheduling_groups;
    if (!_scheduling_groups) {
        _scheduling_groups.emplace();
        _scheduling_groups->compaction_scheduling_group = co_await create_scheduling_group("compaction", 1000);
        _scheduling_groups->memory_compaction_scheduling_group = co_await create_scheduling_group("mem_compaction", 1000);
        _scheduling_groups->streaming_scheduling_group = co_await create_scheduling_group("streaming", 200);
        _scheduling_groups->statement_scheduling_group = co_await create_scheduling_group("statement", 1000);
        _scheduling_groups->memtable_scheduling_group = co_await create_scheduling_group("memtable", 1000);
        _scheduling_groups->memtable_to_cache_scheduling_group = co_await create_scheduling_group("memtable_to_cache", 200);
        _scheduling_groups->gossip_scheduling_group = co_await create_scheduling_group("gossip", 1000);
    }
    co_return *_scheduling_groups;
}

cql_test_config::cql_test_config()
    : cql_test_config(make_shared<db::config>())
{}

cql_test_config::cql_test_config(shared_ptr<db::config> cfg)
    : db_config(cfg)
{
    // This causes huge amounts of commitlog writes to allocate space on disk,
    // which all get thrown away when the test is done. This can cause timeouts
    // if /tmp is not tmpfs.
    db_config->commitlog_use_o_dsync.set(false);

    db_config->add_cdc_extension();
    db_config->add_per_partition_rate_limit_extension();

    db_config->flush_schema_tables_after_modification.set(false);
    db_config->commitlog_use_o_dsync(false);
}

cql_test_config::cql_test_config(const cql_test_config&) = default;
cql_test_config::~cql_test_config() = default;

static const sstring testing_superuser = "tester";

// END TODO

data_dictionary::database
cql_test_env::data_dictionary() {
    return db().local().as_data_dictionary();
}

class single_node_cql_env : public cql_test_env {
public:
    static constexpr std::string_view ks_name = "ks";
    static std::atomic<bool> active;
private:
    service::services_controller _sctl;
    // FIXME: handle signals (SIGINT, SIGTERM) - request aborts
    service::sharded_service_ctl<abort_source> _abort_source_ctl;
    service::sharded_service_ctl<replica::database> _db_ctl;
    service::sharded_service_ctl<gms::feature_service> _feature_service_ctl;
    service::sharded_service_ctl<sstables::storage_manager> _sstm_ctl;
    service::sharded_service_ctl<service::storage_proxy> _proxy_ctl;
    service::sharded_service_ctl<cql3::query_processor> _qp_ctl;
    service::service_ctl<void> _qp_remote_ctl;
    service::sharded_service_ctl<auth::service> _auth_service_ctl;
    service::sharded_service_ctl<db::view::view_builder> _view_builder_ctl;
    service::sharded_service_ctl<db::view::view_update_generator> _view_update_generator_ctl;
    service::sharded_service_ctl<service::migration_notifier> _mnotifier_ctl;
    service::sharded_service_ctl<qos::service_level_controller> _sl_controller_ctl;
    service::sharded_service_ctl<service::migration_manager> _mm_ctl;
    service::sharded_service_ctl<db::batchlog_manager> _batchlog_manager_ctl;
    service::sharded_service_ctl<gms::gossiper> _gossiper_ctl;
    service::sharded_service_ctl<service::raft_group_registry> _group0_registry_ctl;
    service::sharded_service_ctl<db::system_keyspace> _sys_ks_ctl;
    service::sharded_service_ctl<service::tablet_allocator> _tablet_allocator_ctl;
    service::sharded_service_ctl<db::system_distributed_keyspace> _sys_dist_ks_ctl;
    service::sharded_service_ctl<locator::snitch_ptr> _snitch_ctl;
    service::sharded_service_ctl<compaction_manager> _cm_ctl;
    service::sharded_service_ctl<tasks::task_manager> _task_manager_ctl;
    service::sharded_service_ctl<netw::messaging_service> _ms_ctl;
    service::sharded_service_ctl<service::storage_service> _ss_ctl;
    service::sharded_service_ctl<locator::shared_token_metadata> _token_metadata_ctl;
    service::sharded_service_ctl<locator::effective_replication_map_factory> _erm_factory_ctl;
    service::sharded_service_ctl<sstables::directory_semaphore> _sst_dir_semaphore_ctl;
    service::sharded_service_ctl<wasm::manager> _wasm_ctl;
    service::service_ctl<wasm::startup_context> _wasm_ctx_ctl;
    service::sharded_service_ctl<cql3::cql_config> _cql_config_ctl;
    service::sharded_service_ctl<service::endpoint_lifecycle_notifier> _elc_notif_ctl;
    service::sharded_service_ctl<cdc::generation_service> _cdc_generation_service_ctl;
    service::sharded_service_ctl<repair_service> _repair_ctl;
    service::sharded_service_ctl<streaming::stream_manager> _stream_manager_ctl;
    service::sharded_service_ctl<service::forward_service> _forward_service_ctl;
    service::direct_fd_clock _fd_clock;
    service::sharded_service_ctl<direct_failure_detector::failure_detector> _fd_ctl;
    service::sharded_service_ctl<service::raft_address_map> _raft_address_map_ctl;
    service::sharded_service_ctl<service::direct_fd_pinger> _fd_pinger_ctl;
    service::sharded_service_ctl<cdc::cdc_service> _cdc_ctl;
    service::service_ctl<scheduling_groups> _scheduling_groups_ctl;
    service::service_ctl<replica::database_config> _db_cfg_ctl;
    service::service_ctl<configurable::notify_set> _notify_set_ctl;
    service::service_ctl<locator::host_id> _host_id_ctl;
    service::service_ctl<service::raft_group0_client> _raft_group0_client_ctl;
    service::service_ctl<service::raft_group0> _raft_group0_ctl;

    service::raft_group0_client* _group0_client;

private:
    struct core_local_state {
        service::client_state client_state;

        core_local_state(auth::service& auth_service, qos::service_level_controller& sl_controller)
            : client_state(service::client_state::external_tag{}, auth_service, &sl_controller, infinite_timeout_config)
        {
            client_state.set_login(auth::authenticated_user(testing_superuser));
        }

        future<> stop() {
            return make_ready_future<>();
        }
    };
    distributed<core_local_state> _core_local;
private:
    auto make_query_state() {
        if (_db_ctl.local().has_keyspace(ks_name)) {
            _core_local.local().client_state.set_keyspace(_db_ctl.local(), ks_name);
        }
        return ::make_shared<service::query_state>(_core_local.local().client_state, empty_service_permit());
    }
    static void adjust_rlimit() {
        // Tests should use 1024 file descriptors, but don't punish them
        // with weird behavior if they do.
        //
        // Since this more of a courtesy, don't make the situation worse if
        // getrlimit/setrlimit fail for some reason.
        struct rlimit lim;
        int r = getrlimit(RLIMIT_NOFILE, &lim);
        if (r == -1) {
            return;
        }
        if (lim.rlim_cur < lim.rlim_max) {
            lim.rlim_cur = lim.rlim_max;
            setrlimit(RLIMIT_NOFILE, &lim);
        }
    }
public:
    single_node_cql_env()
        : _abort_source_ctl(_sctl, "abort_source", service::default_start_tag{})
        , _db_ctl(_sctl, "db")
        , _feature_service_ctl(_sctl, "feature_service")
        , _sstm_ctl(_sctl, "sstables::storage_manager")
        , _proxy_ctl(_sctl, "storage_proxy")
        , _qp_ctl(_sctl, "cql3::query_processor")
        , _qp_remote_ctl(_sctl, "qp_remote")
        , _auth_service_ctl(_sctl, "auth")
        , _view_builder_ctl(_sctl, "view_builder")
        , _view_update_generator_ctl(_sctl, "view_update_generator")
        , _mnotifier_ctl(_sctl, "migration_notifier", service::default_start_tag{})
        , _sl_controller_ctl(_sctl, "service_level_controller")
        , _mm_ctl(_sctl, "migration_manager")
        , _batchlog_manager_ctl(_sctl, "batchlog_manager")
        , _gossiper_ctl(_sctl, "gossiper")
        , _group0_registry_ctl(_sctl, "raft_group_registry")
        , _sys_ks_ctl(_sctl, "system_keyspace")
        , _tablet_allocator_ctl(_sctl, "tablet_allocator")
        , _sys_dist_ks_ctl(_sctl, "system_distributed_keyspace")
        , _snitch_ctl(_sctl, "snitch")
        , _cm_ctl(_sctl, "compaction_manager")
        , _task_manager_ctl(_sctl, "task_manager")
        , _ms_ctl(_sctl, "messaging_service")
        , _ss_ctl(_sctl, "storage_service")
        , _token_metadata_ctl(_sctl, "shared_token_metadata")
        , _erm_factory_ctl(_sctl, "effective_replication_map_factory", service::default_start_tag{})
        , _sst_dir_semaphore_ctl(_sctl, "directory_semaphore")
        , _wasm_ctl(_sctl, "wasm")
        , _wasm_ctx_ctl(_sctl, "wasm_startup_context")
        , _cql_config_ctl(_sctl, "cql_config")
        , _elc_notif_ctl(_sctl, "endpoint_lifecycle_notifier", service::default_start_tag{})
        , _cdc_generation_service_ctl(_sctl, "generation_service")
        , _repair_ctl(_sctl, "repair")
        , _stream_manager_ctl(_sctl, "stream_manager")
        , _forward_service_ctl(_sctl, "forward_service")
        , _fd_ctl(_sctl, "failure_detector")
        , _raft_address_map_ctl(_sctl, "raft_address_map", service::default_start_tag{})
        , _fd_pinger_ctl(_sctl, "fd_pinger")
        , _cdc_ctl(_sctl, "cdc")
        , _scheduling_groups_ctl(_sctl, "scheduling_groups")
        , _db_cfg_ctl(_sctl, "db_cfg")
        , _notify_set_ctl(_sctl, "notify_set")
        , _host_id_ctl(_sctl, "host_id")
        , _raft_group0_client_ctl(_sctl, "raft_group0_client")
        , _raft_group0_ctl(_sctl, "raft_group0")
    {
        adjust_rlimit();
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(sstring_view text) override {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        auto qo = make_shared<cql3::query_options>(cql3::query_options::DEFAULT);
        return local_qp().execute_direct_without_checking_exception_message(text, *qs, *qo).then([qs, qo] (auto msg) {
            return cql_transport::messages::propagate_exception_as_future(std::move(msg));
        });
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(
        sstring_view text,
        std::unique_ptr<cql3::query_options> qo) override
    {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_direct_without_checking_exception_message(text, *qs, lqo).then([qs, qo = std::move(qo)] (auto msg) {
            return cql_transport::messages::propagate_exception_as_future(std::move(msg));
        });
    }

    virtual future<cql3::prepared_cache_key_type> prepare(sstring query) override {
        return qp().invoke_on_all([query, this] (auto& local_qp) {
            auto qs = this->make_query_state();
            return local_qp.prepare(query, *qs).finally([qs] {}).discard_result();
        }).then([query, this] {
            return local_qp().compute_id(query, ks_name);
        });
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_prepared(
        cql3::prepared_cache_key_type id,
        cql3::raw_value_vector_with_unset values,
        db::consistency_level cl = db::consistency_level::ONE) override {

        const auto& so = cql3::query_options::specific_options::DEFAULT;
        auto options = std::make_unique<cql3::query_options>(cl,
                std::move(values), cql3::query_options::specific_options{
                            so.page_size,
                            so.state,
                            db::consistency_level::SERIAL,
                            so.timestamp,
                        });
        return execute_prepared_with_qo(id, std::move(options));
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_prepared_with_qo(
        cql3::prepared_cache_key_type id,
        std::unique_ptr<cql3::query_options> qo) override
    {
        auto prepared = local_qp().get_prepared(id);
        if (!prepared) {
            throw not_prepared_exception(id);
        }
        auto stmt = prepared->statement;

        assert(stmt->get_bound_terms() == qo->get_values_count());
        qo->prepare(prepared->bound_names);

        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_prepared_without_checking_exception_message(*qs, std::move(stmt), lqo, std::move(prepared), std::move(id), true)
            .then([qs, qo = std::move(qo)] (auto msg) {
                return cql_transport::messages::propagate_exception_as_future(std::move(msg));
            });
    }

    virtual future<std::vector<mutation>> get_modification_mutations(const sstring& text) override {
        auto qs = make_query_state();
        auto cql_stmt = local_qp().get_statement(text, qs->get_client_state())->statement;
        auto modif_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(std::move(cql_stmt));
        if (!modif_stmt) {
            throw std::runtime_error(format("get_stmt_mutations: not a modification statement: {}", text));
        }
        auto& qo = cql3::query_options::DEFAULT;
        auto timeout = db::timeout_clock::now() + qs->get_client_state().get_timeout_config().write_timeout;

        return modif_stmt->get_mutations(local_qp(), qo, timeout, false, qo.get_timestamp(*qs), *qs)
            .finally([qs, modif_stmt = std::move(modif_stmt)] {});
    }

    virtual future<> create_table(std::function<schema(std::string_view)> schema_maker) override {
        schema_builder builder(make_lw_shared<schema>(schema_maker(ks_name)));
        auto s = builder.build(schema_builder::compact_storage::no);
        auto group0_guard = co_await _mm_ctl.local().start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        co_return co_await _mm_ctl.local().announce(co_await service::prepare_new_column_family_announcement(_proxy_ctl.local(), s, ts), std::move(group0_guard), "");
    }

    virtual service::client_state& local_client_state() override {
        return _core_local.local().client_state;
    }

    virtual replica::database& local_db() override {
        return _db_ctl.local();
    }

    cql3::query_processor& local_qp() override {
        return _qp_ctl.local();
    }

    sharded<replica::database>& db() override {
        return _db_ctl.service();
    }

    distributed<cql3::query_processor>& qp() override {
        return _qp_ctl.service();
    }

    auth::service& local_auth_service() override {
        return _auth_service_ctl.local();
    }

    virtual db::view::view_builder& local_view_builder() override {
        return _view_builder_ctl.local();
    }

    virtual db::view::view_update_generator& local_view_update_generator() override {
        return _view_update_generator_ctl.local();
    }

    virtual service::migration_notifier& local_mnotifier() override {
        return _mnotifier_ctl.local();
    }

    virtual sharded<service::migration_manager>& migration_manager() override {
        return _mm_ctl.service();
    }

    virtual sharded<db::batchlog_manager>& batchlog_manager() override {
        return _batchlog_manager_ctl.service();
    }

    virtual sharded<gms::gossiper>& gossiper() override {
        return _gossiper_ctl.service();
    }

    virtual service::raft_group0_client& get_raft_group0_client() override {
        return *_group0_client;
    }

    virtual sharded<service::raft_group_registry>& get_raft_group_registry() override {
        return _group0_registry_ctl.service();
    }

    virtual sharded<db::system_keyspace>& get_system_keyspace() override {
        return _sys_ks_ctl.service();
    }

    virtual sharded<service::tablet_allocator>& get_tablet_allocator() override {
        return _tablet_allocator_ctl.service();
    }

    virtual sharded<service::storage_proxy>& get_storage_proxy() override {
        return _proxy_ctl.service();
    }

    virtual sharded<gms::feature_service>& get_feature_service() override {
        return _feature_service_ctl.service();
    }

    virtual sharded<sstables::storage_manager>& get_sstorage_manager() override {
        return _sstm_ctl.service();
    }

    virtual future<> refresh_client_state() override {
        return _core_local.invoke_on_all([] (core_local_state& state) {
            return state.client_state.maybe_update_per_service_level_params();
        });
    }

    future<> create_keyspace(const cql_test_config& cfg, std::string_view name) {
        auto query = format("create keyspace {} with replication = {{ 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor' : 1{}}};", name,
                            cfg.initial_tablets ? format(", 'initial_tablets' : {}", *cfg.initial_tablets) : "");
        return execute_cql(query).discard_result();
    }

    static future<> do_with(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in, std::optional<cql_test_init_configurables> init_configurables) {
        return seastar::async([cfg_in = std::move(cfg_in), init_configurables = std::move(init_configurables), func] {
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            bool old_active = false;
            if (!active.compare_exchange_strong(old_active, true)) {
                throw std::runtime_error("Starting more than one cql_test_env at a time not supported due to singletons.");
            }
            auto deactivate = defer([] {
                bool old_active = true;
                auto success = active.compare_exchange_strong(old_active, false);
                assert(success);
            });

            // FIXME: make the function storage non static
            auto clear_funcs = defer([] {
                smp::invoke_on_all([] () {
                    cql3::functions::functions::clear_functions();
                }).get();
            });

            utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
            utils::fb_utilities::set_broadcast_rpc_address(gms::inet_address("localhost"));

            single_node_cql_env env;
            env.run_in_thread(std::move(func), std::move(cfg_in), std::move(init_configurables));
        });
    }

private:
    void run_in_thread(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in, std::optional<cql_test_init_configurables> init_configurables) {
            using namespace std::filesystem;

            // disable reactor stall detection during startup
            auto blocked_reactor_notify_ms = engine().get_blocked_reactor_notify_ms();
            smp::invoke_on_all([] {
                engine().update_blocked_reactor_notify_ms(std::chrono::milliseconds(1000000));
            }).get();

            set_abort_on_internal_error(true);
            const gms::inet_address listen("127.0.0.1");

            auto cfg = cfg_in.db_config;
            if (!cfg->reader_concurrency_semaphore_serialize_limit_multiplier.is_set()) {
                cfg->reader_concurrency_semaphore_serialize_limit_multiplier.set(std::numeric_limits<uint32_t>::max());
            }
            if (!cfg->reader_concurrency_semaphore_kill_limit_multiplier.is_set()) {
                cfg->reader_concurrency_semaphore_kill_limit_multiplier.set(std::numeric_limits<uint32_t>::max());
            }
            tmpdir data_dir;
            auto data_dir_path = data_dir.path().string();
            if (!cfg->data_file_directories.is_set()) {
                cfg->data_file_directories.set({data_dir_path});
            } else {
                data_dir_path = cfg->data_file_directories()[0];
            }
            cfg->commitlog_directory.set(data_dir_path + "/commitlog.dir");
            cfg->schema_commitlog_directory.set(cfg->commitlog_directory() + "/schema");
            cfg->hints_directory.set(data_dir_path + "/hints.dir");
            cfg->view_hints_directory.set(data_dir_path + "/view_hints.dir");
            cfg->num_tokens.set(256);
            cfg->ring_delay_ms.set(500);
            cfg->shutdown_announce_in_ms.set(0);
            cfg->broadcast_to_all_shards().get();
            create_directories((data_dir_path + "/system").c_str());
            create_directories(cfg->commitlog_directory().c_str());
            create_directories(cfg->schema_commitlog_directory().c_str());
            create_directories(cfg->hints_directory().c_str());
            create_directories(cfg->view_hints_directory().c_str());
            for (unsigned i = 0; i < smp::count; ++i) {
                create_directories((cfg->hints_directory() + "/" + std::to_string(i)).c_str());
                create_directories((cfg->view_hints_directory() + "/" + std::to_string(i)).c_str());
            }

            if (!cfg->max_memory_for_unlimited_query_soft_limit.is_set()) {
                cfg->max_memory_for_unlimited_query_soft_limit.set(uint64_t(query::result_memory_limiter::unlimited_result_size));
            }
            if (!cfg->max_memory_for_unlimited_query_hard_limit.is_set()) {
                cfg->max_memory_for_unlimited_query_hard_limit.set(uint64_t(query::result_memory_limiter::unlimited_result_size));
            }

            _scheduling_groups_ctl.start_func = [&] () {
                return get_scheduling_groups();
            };

            _notify_set_ctl.start_func = [&,
                db = _notify_set_ctl.lookup_dep(_db_ctl),
                ss = _notify_set_ctl.lookup_dep(_ss_ctl),
                mm = _notify_set_ctl.lookup_dep(_mm_ctl),
                proxy = _notify_set_ctl.lookup_dep(_proxy_ctl),
                feature_service = _notify_set_ctl.lookup_dep(_feature_service_ctl),
                ms = _notify_set_ctl.lookup_dep(_ms_ctl),
                qp = _notify_set_ctl.lookup_dep(_qp_ctl),
                bm = _notify_set_ctl.lookup_dep(_batchlog_manager_ctl)
            ] () -> future<configurable::notify_set> {
                if (init_configurables) {
                    co_return co_await configurable::init_all(*cfg, init_configurables->extensions,
                            service_set(db.get(), ss.get(), mm.get(), proxy.get(), feature_service.get(), ms.get(), qp.get(), bm.get()));
                } else {
                    co_return configurable::notify_set{};
                }
            };
            _notify_set_ctl.stop_func = [] (configurable::notify_set& s) {
                return s.notify_all(configurable::system_state::stopped);
            };

            gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg, cfg_in.disabled_features);
            _feature_service_ctl.start_func = [&fcfg] (auto& fs) { return fs.start(fcfg); };
            _feature_service_ctl.serve_func = [] (auto& fs) {
                return fs.invoke_on_all([] (auto& fs) {
                    return fs.enable(fs.supported_feature_set());
                });
            };

            _snitch_ctl.start_func = [] (auto& s) -> future<> {
                co_await s.start(locator::snitch_config{});
                co_await s.invoke_on_all(&locator::snitch_ptr::start);
            };

            locator::token_metadata::config tm_cfg;
            tm_cfg.topo_cfg.this_endpoint = utils::fb_utilities::get_broadcast_address();
            tm_cfg.topo_cfg.local_dc_rack = { _snitch_ctl.local()->get_datacenter(), _snitch_ctl.local()->get_rack() };
            _token_metadata_ctl.start_func = [&] (auto& s) {
                return s.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg);
            };

            _sst_dir_semaphore_ctl.start_func = [&] (auto& s) { return s.start(cfg->initial_sstable_loading_concurrency()); };

            _db_cfg_ctl.start_func = [&,
                sg_ref = _db_ctl.lookup_dep(_scheduling_groups_ctl)
            ] () {
                replica::database_config dbcfg;
                if (cfg_in.dbcfg) {
                    dbcfg = std::move(*cfg_in.dbcfg);
                } else {
                    dbcfg.available_memory = memory::stats().total_memory();
                }
                const auto& sg = sg_ref.get();
                dbcfg.compaction_scheduling_group = sg->compaction_scheduling_group;
                dbcfg.memory_compaction_scheduling_group = sg->memory_compaction_scheduling_group;
                dbcfg.streaming_scheduling_group = sg->streaming_scheduling_group;
                dbcfg.statement_scheduling_group = sg->statement_scheduling_group;
                dbcfg.memtable_scheduling_group = sg->memtable_scheduling_group;
                dbcfg.memtable_to_cache_scheduling_group = sg->memtable_to_cache_scheduling_group;
                dbcfg.gossip_scheduling_group = sg->gossip_scheduling_group;
                dbcfg.sstables_format = sstables::version_from_string(cfg->sstable_format());
                return make_ready_future<replica::database_config>(std::move(dbcfg));
            };

            _db_ctl.start_func = [&, cfg = std::ref(*cfg),
                db_cfg = _db_ctl.lookup_dep(_db_cfg_ctl),
                mn = _db_ctl.lookup_dep(_mnotifier_ctl),
                feature_service = _db_ctl.lookup_dep(_feature_service_ctl),
                token_metadata = _db_ctl.lookup_dep(_token_metadata_ctl),
                cm = _db_ctl.lookup_dep(_cm_ctl),
                sstm = _db_ctl.lookup_dep(_sstm_ctl),
                sst_dir_semaphore = _db_ctl.lookup_dep(_sst_dir_semaphore_ctl)
            ] (auto& db) -> future<> {
                co_await db.start(cfg, *db_cfg.get(), mn, feature_service, token_metadata, cm, sstm, sst_dir_semaphore);
                debug::the_database = &db;
                co_await db.invoke_on_all(&replica::database::start);
            };
            _db_ctl.shutdown_func = [] (auto& db) {
                return db.invoke_on_all(&replica::database::shutdown);
            };
            _db_ctl.stop_func = [] (auto& db) -> future<> {
                co_await db.stop();
                debug::the_database = nullptr;
            };

            auto get_tm_cfg = sharded_parameter([&] {
                return tasks::task_manager::config {
                    .task_ttl = cfg->task_ttl_seconds,
                };
            });
            _task_manager_ctl.start_func = [&,
                as = _task_manager_ctl.lookup_dep(_abort_source_ctl)
            ] (auto& tm) {
                return tm.start(std::move(get_tm_cfg), as);
            };

            _cm_ctl.start_func = [&,
                db_cfg = _cm_ctl.lookup_dep(_db_cfg_ctl),
                as = _cm_ctl.lookup_dep(_abort_source_ctl),
                tm = _cm_ctl.lookup_dep(_task_manager_ctl)
            ] (auto& cm) {
                // get_cm_cfg is called on each shard when starting a service::sharded_service_ctl<compaction_manager>
                // we need the getter since updateable_value is not shard-safe (#7316)
                auto get_cm_cfg = sharded_parameter([&] {
                    return compaction_manager::config {
                        .compaction_sched_group = compaction_manager::scheduling_group{db_cfg.get()->compaction_scheduling_group},
                        .maintenance_sched_group = compaction_manager::scheduling_group{db_cfg.get()->streaming_scheduling_group},
                        .available_memory = db_cfg.get()->available_memory,
                        .static_shares = cfg->compaction_static_shares,
                        .throughput_mb_per_sec = cfg->compaction_throughput_mb_per_sec,
                    };
                });
                return cm.start(get_cm_cfg, as, tm);
            };
            _cm_ctl.serve_func = [this] (auto& cm) {
                return cm.invoke_on_all([&](compaction_manager& cm) {
                    auto cl = local_db().commitlog();
                    auto scl = local_db().schema_commitlog();
                    if (cl && scl) {
                        cm.get_tombstone_gc_state().set_gc_time_min_source([cl, scl](const table_id& id) {
                            return std::min(cl->min_gc_time(id), scl->min_gc_time(id));
                        });
                    } else if (cl) {
                        cm.get_tombstone_gc_state().set_gc_time_min_source([cl](const table_id& id) {
                            return cl->min_gc_time(id);
                        });
                    } else if (scl) {
                        cm.get_tombstone_gc_state().set_gc_time_min_source([scl](const table_id& id) {
                            return scl->min_gc_time(id);
                        });
                    }
                });
            };

            _sstm_ctl.start_func = [&] (auto& s) {
                return s.start(std::ref(*cfg), sstables::storage_manager::config{});
            };

            if (cfg->enable_user_defined_functions() && cfg->check_experimental(db::experimental_features_t::feature::UDF)) {
                _wasm_ctx_ctl.start_func = [&, db_cfg = _cm_ctl.lookup_dep(_db_cfg_ctl)] {
                    return make_ready_future<wasm::startup_context>(*cfg, *db_cfg.get());
                };
            }

            _wasm_ctl.start_func = [&, wasm_ctx = _wasm_ctl.lookup_dep(_wasm_ctx_ctl)] (auto& s) {
                return s.start(wasm_ctx);
            };

            service::storage_proxy::config spcfg {
                .hints_directory_initializer = db::hints::directory_initializer::make_dummy(),
            };
            spcfg.available_memory = memory::stats().total_memory();
            db::view::node_update_backlog b(smp::count, 10ms);
            scheduling_group_key_config sg_conf =
                    make_scheduling_group_key_config<service::storage_proxy_stats::stats>();
            _proxy_ctl.start_func = [&,
                db = _proxy_ctl.lookup_dep(_db_ctl),
                feature_service = _proxy_ctl.lookup_dep(_feature_service_ctl),
                tm = _proxy_ctl.lookup_dep(_token_metadata_ctl),
                erm_factory = _proxy_ctl.lookup_dep(_erm_factory_ctl)
            ] (auto& sp) {
                return sp.start(db, spcfg, std::ref(b), scheduling_group_key_create(sg_conf).get0(), feature_service, tm, erm_factory);
            };

            _cql_config_ctl.start_func = [] (auto& s) { return s.start(cql3::cql_config::default_tag{}); };

            cql3::query_processor::memory_config qp_mcfg;
            if (cfg_in.qp_mcfg) {
                qp_mcfg = *cfg_in.qp_mcfg;
            } else {
                qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
            }

            utils::loading_cache_config auth_prep_cache_config;
            auth_prep_cache_config.max_size = qp_mcfg.authorized_prepared_cache_size;
            auth_prep_cache_config.expiry = std::min(std::chrono::milliseconds(cfg->permissions_validity_in_ms()),
                                                     std::chrono::duration_cast<std::chrono::milliseconds>(cql3::prepared_statements_cache::entry_expiry));
            auth_prep_cache_config.refresh = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            _qp_ctl.start_func = [&,
                proxy = _qp_ctl.lookup_dep(_proxy_ctl),
                local_data_dict = seastar::sharded_parameter([] (const replica::database& db) { return db.as_data_dictionary(); }, _qp_ctl.lookup_dep(_db_ctl)),
                mn = _qp_ctl.lookup_dep(_mnotifier_ctl),
                cql_config = _qp_ctl.lookup_dep(_cql_config_ctl),
                wasm = _qp_ctl.lookup_dep(_wasm_ctl)
            ] (auto& qp) mutable {
                return qp.start(proxy, std::move(local_data_dict), mn, qp_mcfg, cql_config, auth_prep_cache_config, wasm);
            };

            _sl_controller_ctl.start_func = [&,
                auth_service = _sl_controller_ctl.lookup_dep(_auth_service_ctl)
            ] (auto& slc) -> future<> {
                co_await slc.start(auth_service, qos::service_level_options{});
                co_await slc.invoke_on_all(&qos::service_level_controller::start);
            };

            _sys_ks_ctl.start_func = [&,
                qp = _sys_ks_ctl.lookup_dep(_qp_ctl),
                erm_factory = _sys_ks_ctl.lookup_dep(_erm_factory_ctl),
                db = _sys_ks_ctl.lookup_dep(_db_ctl)
            ] (auto& sys_ks) -> future<> {
                co_await sys_ks.start(qp, db);
                replica::distributed_loader::init_system_keyspace(sys_ks, erm_factory.get(), db.get()).get();
                db.get().local().maybe_init_schema_commitlog();
                co_await sys_ks.invoke_on_all(&db::system_keyspace::mark_writable);
            };

            _host_id_ctl.start_func = [&,
                sys_ks = _host_id_ctl.lookup_dep(_sys_ks_ctl),
                tm = _host_id_ctl.lookup_dep(_token_metadata_ctl)
            ] () -> future<locator::host_id> {
                auto host_id = cfg_in.host_id;
                if (!host_id) {
                    auto linfo = co_await sys_ks.get().local().load_local_info();
                    if (!linfo.host_id) {
                        linfo.host_id = locator::host_id::create_random_id();
                    }
                    host_id = linfo.host_id;
                    co_await sys_ks.get().local().save_local_info(std::move(linfo), _snitch_ctl.local()->get_location());
                }
                co_await locator::shared_token_metadata::mutate_on_all_shards(tm.get(), [hostid = host_id] (locator::token_metadata& tm) {
                    auto& topo = tm.get_topology();
                    topo.set_host_id_cfg(hostid);
                    topo.add_or_update_endpoint(utils::fb_utilities::get_broadcast_address(),
                                                hostid,
                                                std::nullopt,
                                                locator::node::state::normal,
                                                smp::count);
                    return make_ready_future<>();
                });
                co_return host_id;
            };

            // don't start listening so tests can be run in parallel
            _ms_ctl.start_func = [&, host_id = _ms_ctl.lookup_dep(_host_id_ctl)] (auto& ms) {
                return ms.start(*host_id.get(), listen, std::move(7000));
            };

            // Init gossiper
            std::set<gms::inet_address> seeds;
            auto seed_provider = db::config::seed_provider_type();
            if (seed_provider.parameters.contains("seeds")) {
                size_t begin = 0;
                size_t next = 0;
                sstring seeds_str = seed_provider.parameters.find("seeds")->second;
                while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
                    seeds.emplace(gms::inet_address(seeds_str.substr(begin,next-begin)));
                    begin = next+1;
                }
            }
            if (seeds.empty()) {
                seeds.emplace(gms::inet_address("127.0.0.1"));
            }

            gms::gossip_config gcfg;
            gcfg.cluster_name = "Test Cluster";
            gcfg.seeds = std::move(seeds);
            gcfg.skip_wait_for_gossip_to_settle = 0;
            _gossiper_ctl.start_func = [&,
                as = _gossiper_ctl.lookup_dep(_abort_source_ctl),
                tm = _gossiper_ctl.lookup_dep(_token_metadata_ctl),
                ms = _gossiper_ctl.lookup_dep(_ms_ctl)
            ] (auto& g) -> future<> {
                co_await g.start(as, tm, ms, std::ref(*cfg), gcfg);
                co_await g.invoke_on_all(&gms::gossiper::start);
            };

            _fd_pinger_ctl.start_func = [&,
                ms = _fd_pinger_ctl.lookup_dep(_ms_ctl),
                am = _fd_pinger_ctl.lookup_dep(_raft_address_map_ctl)
            ] (auto& fd_pinger) {
                return fd_pinger.start(ms, am);
            };

            _fd_ctl.start_func = [&,
                fd_pinger = _fd_ctl.lookup_dep(_fd_pinger_ctl)
            ] (auto& fd) {
                return fd.start(fd_pinger, _fd_clock,
                        service::direct_fd_clock::base::duration{std::chrono::milliseconds{100}}.count());
            };

            _group0_registry_ctl.start_func = [&,
                host_id = _group0_registry_ctl.lookup_dep(_host_id_ctl),
                am = _fd_pinger_ctl.lookup_dep(_raft_address_map_ctl),
                ms = _fd_pinger_ctl.lookup_dep(_ms_ctl),
                gossiper = _fd_pinger_ctl.lookup_dep(_gossiper_ctl),
                fd = _fd_pinger_ctl.lookup_dep(_fd_ctl)
            ] (auto& raft_gr) {
                return raft_gr.start(cfg->consistent_cluster_management(),
                    raft::server_id{host_id.get()->id}, am, ms, gossiper, fd);
            };

            _stream_manager_ctl.start_func = [&,
                db = _fd_pinger_ctl.lookup_dep(_db_ctl),
                sys_dist_ks = _fd_pinger_ctl.lookup_dep(_sys_dist_ks_ctl),
                vug = _fd_pinger_ctl.lookup_dep(_view_update_generator_ctl),
                ms = _fd_pinger_ctl.lookup_dep(_ms_ctl),
                mm = _fd_pinger_ctl.lookup_dep(_mm_ctl),
                gossiper = _fd_pinger_ctl.lookup_dep(_gossiper_ctl),
                sg = _fd_pinger_ctl.lookup_dep(_scheduling_groups_ctl)
            ] (auto& sm) -> future<> {
                return sm.start(std::ref(*cfg), db, sys_dist_ks, vug, ms, mm, gossiper, sg.get()->streaming_scheduling_group);
            };

            _forward_service_ctl.start_func = [&,
                ms = _forward_service_ctl.lookup_dep(_ms_ctl),
                proxy = _forward_service_ctl.lookup_dep(_proxy_ctl),
                db = _forward_service_ctl.lookup_dep(_db_ctl),
                tm = _forward_service_ctl.lookup_dep(_token_metadata_ctl),
                as = _forward_service_ctl.lookup_dep(_abort_source_ctl)
            ] (auto& fs) {
                return fs.start(ms, proxy, db, tm, as);
            };

            // group0 client exists only on shard 0
            _raft_group0_client_ctl.start_func = [&,
                raft_gr = _raft_group0_client_ctl.lookup_dep(_group0_registry_ctl),
                sys_ks = _raft_group0_client_ctl.lookup_dep(_sys_ks_ctl)
            ] () {
                return make_ready_future<service::raft_group0_client>(raft_gr.get().local(), sys_ks.get().local());
            };

            _mm_ctl.start_func = [&,
                mn = _mm_ctl.lookup_dep(_mnotifier_ctl),
                fs = _mm_ctl.lookup_dep(_feature_service_ctl),
                ms = _fd_pinger_ctl.lookup_dep(_ms_ctl),
                proxy = _mm_ctl.lookup_dep(_proxy_ctl),
                gossiper = _mm_ctl.lookup_dep(_gossiper_ctl),
                group0_client = _mm_ctl.lookup_dep(_raft_group0_client_ctl),
                sys_ks = _mm_ctl.lookup_dep(_sys_ks_ctl)
            ] (auto& mm) {
                return mm.start(mn, fs, ms, proxy, gossiper, *group0_client.get(), sys_ks);
            };

            _tablet_allocator_ctl.start_func = [&,
                mn = _tablet_allocator_ctl.lookup_dep(_mnotifier_ctl),
                db = _tablet_allocator_ctl.lookup_dep(_db_ctl)
            ] (auto& ta) -> future<> {
                co_await ta.start(mn, db);
            };

            _qp_remote_ctl.start_func = [&,
                ta = _qp_remote_ctl.lookup_dep(_tablet_allocator_ctl),
                mm = _qp_remote_ctl.lookup_dep(_mm_ctl),
                qp = _qp_remote_ctl.lookup_dep(_qp_ctl),
                fs = _qp_remote_ctl.lookup_dep(_forward_service_ctl),
                group0_client = _qp_remote_ctl.lookup_dep(_raft_group0_client_ctl)
            ] () {
                return qp.get().invoke_on_all([&] (cql3::query_processor& qp) {
                    qp.start_remote(mm.get().local(), fs.get().local(), *group0_client.get());
                });
            };
            _qp_remote_ctl.stop_func = [&] {
                return _qp_ctl.service().invoke_on_all(&cql3::query_processor::stop_remote);
            };

            _raft_group0_ctl.start_func = [&,
                as = _raft_group0_ctl.lookup_dep(_abort_source_ctl),
                raft_gr = _raft_group0_ctl.lookup_dep(_group0_registry_ctl),
                ms = _raft_group0_ctl.lookup_dep(_ms_ctl),
                gossiper = _raft_group0_ctl.lookup_dep(_gossiper_ctl),
                fs = _raft_group0_ctl.lookup_dep(_feature_service_ctl),
                sys_ks = _raft_group0_ctl.lookup_dep(_sys_ks_ctl),
                group0_client = _raft_group0_ctl.lookup_dep(_raft_group0_client_ctl)
            ] () {
                return make_ready_future<service::raft_group0>(
                    as.get().local(), raft_gr.get().local(), ms,
                    gossiper.get().local(), fs.get().local(), sys_ks.get().local(), *group0_client.get());
            };

            _ss_ctl.start_func = [&,
                as = _ss_ctl.lookup_dep(_abort_source_ctl),
                db = _ss_ctl.lookup_dep(_db_ctl),
                gossiper = _ss_ctl.lookup_dep(_gossiper_ctl),
                sys_ks = _ss_ctl.lookup_dep(_sys_ks_ctl),
                fs = _ss_ctl.lookup_dep(_feature_service_ctl),
                mm = _ss_ctl.lookup_dep(_mm_ctl),
                tm = _ss_ctl.lookup_dep(_token_metadata_ctl),
                erm_factory = _ss_ctl.lookup_dep(_erm_factory_ctl),
                ms = _ss_ctl.lookup_dep(_ms_ctl),
                repair = _ss_ctl.lookup_dep(_repair_ctl),
                sm = _ss_ctl.lookup_dep(_stream_manager_ctl),
                elc_notif = _ss_ctl.lookup_dep(_elc_notif_ctl),
                bm = _ss_ctl.lookup_dep(_batchlog_manager_ctl),
                snitch = _ss_ctl.lookup_dep(_snitch_ctl),
                ta = _ss_ctl.lookup_dep(_tablet_allocator_ctl),
                cdc = _ss_ctl.lookup_dep(_cdc_generation_service_ctl)
            ] (auto& ss) {
                return ss.start(as, db, gossiper, sys_ks, fs, mm, tm, erm_factory, ms, repair, sm,
                        elc_notif, bm, snitch, ta, cdc);
            };
            _ss_ctl.serve_func = [&] (auto& ss) -> future<> {
                local_mnotifier().register_listener(&ss.local());
                co_await ss.invoke_on_all([this] (service::storage_service& ss) {
                    ss.set_query_processor(local_qp());
                });
                co_await smp::invoke_on_all([&] {
                    return db::initialize_virtual_tables(db(), ss, gossiper(), get_raft_group_registry(), get_system_keyspace(), *cfg);
                });
            };
            _ss_ctl.drain_func = [this] (auto& ss, service::on_shutdown) {
                return local_mnotifier().unregister_listener(&ss.local());
            };

            replica::distributed_loader::init_non_system_keyspaces(_db_ctl, _proxy_ctl, _sys_ks_ctl).get();

            _db_ctl.invoke_on_all([] (replica::database& db) {
                db.get_tables_metadata().for_each_table([] (table_id, lw_shared_ptr<replica::table> table) {
                    replica::table& t = *table;
                    t.enable_auto_compaction();
                });
            }).get();

            if (_group0_registry_ctl.local().is_enabled()) {
                _group0_registry_ctl.invoke_on_all([] (service::raft_group_registry& raft_gr) {
                    return raft_gr.start();
                }).get();
            }

            group0_client.init().get();
            auto stop_system_keyspace = defer([this] {
                _sys_ks_ctl.invoke_on_all(&db::system_keyspace::shutdown).get();
            });

            auto shutdown_db = defer([this] {
                _db_ctl.invoke_on_all(&replica::database::shutdown).get();
            });
            // XXX: drain_on_shutdown raft before stopping the database and
            // query processor. Group registry stop raft groups
            // when stopped, and until then the groups may use
            // the database and the query processor.
            auto drain_raft = defer([this] {
                _group0_registry_ctl.invoke_on_all(&service::raft_group_registry::drain_on_shutdown).get();
            });

            _view_update_generator_ctl.start(std::ref(_db_ctl), std::ref(_proxy_ctl), std::ref(abort_sources)).get();
            _view_update_generator_ctl.invoke_on_all(&db::view::view_update_generator::start).get();
            auto stop_view_update_generator = defer([this] {
                _view_update_generator_ctl.stop().get();
            });

            _sys_dist_ks_ctl.start(std::ref(_qp_ctl), std::ref(_mm_ctl), std::ref(_proxy_ctl)).get();

            if (cfg_in.need_remote_proxy) {
                _proxy_ctl.invoke_on_all(&service::storage_proxy::start_remote, std::ref(_ms_ctl), std::ref(_gossiper_ctl), std::ref(_mm_ctl), std::ref(_sys_ks_ctl)).get();
            }
            auto stop_proxy_remote = defer([this, need = cfg_in.need_remote_proxy] {
                if (need) {
                    _proxy_ctl.invoke_on_all(&service::storage_proxy::stop_remote).get();
                }
            });

            _sl_controller_ctl.invoke_on_all([this] (qos::service_level_controller& service) {
                qos::service_level_controller::service_level_distributed_data_accessor_ptr service_level_data_accessor =
                        ::static_pointer_cast<qos::service_level_controller::service_level_distributed_data_accessor>(
                                make_shared<qos::unit_test_service_levels_accessor>(_sl_controller_ctl, _sys_dist_ks_ctl));
                return service.set_distributed_data_accessor(std::move(service_level_data_accessor));
            }).get();

            cdc::generation_service::config cdc_config;
            cdc_config.ignore_msb_bits = cfg->murmur3_partitioner_ignore_msb_bits();
            /*
             * Currently used when choosing the timestamp of the first CDC stream generation:
             * normally we choose a timestamp in the future so other nodes have a chance to learn about it
             * before it starts operating, but in the single-node-cluster case this is not necessary
             * and would only slow down tests (by having them wait).
             */
            cdc_config.ring_delay = std::chrono::milliseconds(0);
            _cdc_generation_service_ctl.start(std::ref(cdc_config), std::ref(_gossiper_ctl), std::ref(_sys_dist_ks_ctl), std::ref(_sys_ks_ctl), std::ref(abort_sources), std::ref(_token_metadata_ctl), std::ref(_feature_service_ctl), std::ref(_db_ctl)).get();
            auto stop_cdc_generation_service = defer([this] {
                _cdc_generation_service_ctl.stop().get();
            });

            auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };
            _cdc_ctl.start(std::ref(_proxy_ctl), sharded_parameter(get_cdc_metadata, std::ref(_cdc_generation_service_ctl)), std::ref(_mnotifier_ctl)).get();
            auto stop_cdc_service = defer([this] {
                _cdc_ctl.stop().get();
            });

            group0_service.start().get();
            auto stop_group0_service = defer([&group0_service] {
                group0_service.abort().get();
            });

            const bool raft_topology_change_enabled = group0_service.is_raft_enabled()
                    && cfg->check_experimental(db::experimental_features_t::feature::CONSISTENT_TOPOLOGY_CHANGES);

            _ss_ctl.local().set_group0(group0_service, raft_topology_change_enabled);

            try {
                _ss_ctl.local().join_cluster(_sys_dist_ks_ctl, _proxy_ctl).get();
            } catch (std::exception& e) {
                // if any of the defers crashes too, we'll never see
                // the error
                testlog.error("Failed to join cluster: {}", e);
                throw;
            }

            utils::loading_cache_config perm_cache_config;
            perm_cache_config.max_size = cfg->permissions_cache_max_entries();
            perm_cache_config.expiry = std::chrono::milliseconds(cfg->permissions_validity_in_ms());
            perm_cache_config.refresh = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            const qualified_name qualified_authorizer_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authorizer());
            const qualified_name qualified_authenticator_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authenticator());
            const qualified_name qualified_role_manager_name(auth::meta::AUTH_PACKAGE_NAME, cfg->role_manager());

            auth::service_config auth_config;
            auth_config.authorizer_java_name = qualified_authorizer_name;
            auth_config.authenticator_java_name = qualified_authenticator_name;
            auth_config.role_manager_java_name = qualified_role_manager_name;

            _auth_service_ctl.start(perm_cache_config, std::ref(_qp_ctl), std::ref(_mnotifier_ctl), std::ref(_mm_ctl), auth_config).get();
            _auth_service_ctl.invoke_on_all([this] (auth::service& auth) {
                return auth.start(_mm_ctl.local());
            }).get();

            auto deinit_storage_service_server = defer([this] {
                _gossiper_ctl.invoke_on_all(&gms::gossiper::shutdown).get();
                _auth_service_ctl.stop().get();
            });

            db::batchlog_manager_config bmcfg;
            bmcfg.replay_rate = 100000000;
            bmcfg.write_request_timeout = 2s;
            _batchlog_manager_ctl.start(std::ref(_qp_ctl), std::ref(_sys_ks_ctl), bmcfg).get();
            auto stop_bm = defer([this] {
                _batchlog_manager_ctl.stop().get();
            });

            _view_builder_ctl.start(std::ref(_db_ctl), std::ref(_sys_ks_ctl), std::ref(_sys_dist_ks_ctl), std::ref(_mnotifier_ctl), std::ref(_view_update_generator_ctl)).get();
            _view_builder_ctl.invoke_on_all([this] (db::view::view_builder& vb) {
                return vb.start(_mm_ctl.local());
            }).get();
            auto stop_view_builder = defer([this] {
                _view_builder_ctl.stop().get();
            });

            // Create the testing user.
            try {
                auth::role_config config;
                config.is_superuser = true;
                config.can_login = true;

                auth::create_role(
                        _auth_service_ctl.local(),
                        testing_superuser,
                        config,
                        auth::authentication_options()).get0();
            } catch (const auth::role_already_exists&) {
                // The default user may already exist if this `cql_test_env` is starting with previously populated data.
            }

            notify_set.notify_all(configurable::system_state::started).get();

            _group0_client = &group0_client;

            _core_local.start(std::ref(_auth_service_ctl), std::ref(_sl_controller_ctl)).get();
            auto stop_core_local = defer([this] { _core_local.stop().get(); });

            if (!local_db().has_keyspace(ks_name)) {
                create_keyspace(cfg_in, ks_name).get();
            }

            smp::invoke_on_all([blocked_reactor_notify_ms] {
                engine().update_blocked_reactor_notify_ms(blocked_reactor_notify_ms);
            }).get();

            with_scheduling_group(dbcfg.statement_scheduling_group, [&func, this] {
                return func(*this);
            }).get();
    }

public:
    future<::shared_ptr<cql_transport::messages::result_message>> execute_batch(
        const std::vector<sstring_view>& queries, std::unique_ptr<cql3::query_options> qo) override {
        using cql3::statements::batch_statement;
        using cql3::statements::modification_statement;
        std::vector<batch_statement::single_statement> modifications;
        boost::transform(queries, back_inserter(modifications), [this](const auto& query) {
            auto stmt = local_qp().get_statement(query, _core_local.local().client_state);
            if (!dynamic_cast<modification_statement*>(stmt->statement.get())) {
                throw exceptions::invalid_request_exception(
                    "Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");
            }
            return batch_statement::single_statement(static_pointer_cast<modification_statement>(stmt->statement));
        });
        auto batch = ::make_shared<batch_statement>(
            batch_statement::type::UNLOGGED,
            std::move(modifications),
            cql3::attributes::none(),
            local_qp().get_cql_stats());
        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_batch_without_checking_exception_message(batch, *qs, lqo, {}).then([qs, batch, qo = std::move(qo)] (auto msg) {
            return cql_transport::messages::propagate_exception_as_future(std::move(msg));
        });
    }
};

std::atomic<bool> single_node_cql_env::active = { false };

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in, std::optional<cql_test_init_configurables> init_configurables) {
    return single_node_cql_env::do_with(func, std::move(cfg_in), std::move(init_configurables));
}

future<> do_with_cql_env_thread(std::function<void(cql_test_env&)> func, cql_test_config cfg_in, thread_attributes thread_attr, std::optional<cql_test_init_configurables> init_configurables) {
    return single_node_cql_env::do_with([func = std::move(func), thread_attr] (auto& e) {
        return seastar::async(thread_attr, [func = std::move(func), &e] {
            return func(e);
        });
    }, std::move(cfg_in), std::move(init_configurables));
}

reader_permit make_reader_permit(cql_test_env& env) {
    return env.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "test", db::no_timeout, {});
}

cql_test_config raft_cql_test_config() {
    cql_test_config c;
    c.db_config->consistent_cluster_management(true);
    return c;
}
