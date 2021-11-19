/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <boost/range/algorithm/transform.hpp>
#include <iterator>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include "database_fwd.hh"
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
#include "compaction/compaction_manager.hh"
#include "message/messaging_service.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "auth/service.hh"
#include "auth/common.hh"
#include "db/config.hh"
#include "db/batchlog_manager.hh"
#include "schema_builder.hh"
#include "test/lib/tmpdir.hh"
#include "db/query_context.hh"
#include "test/lib/test_services.hh"
#include "test/lib/log.hh"
#include "unit_test_service_levels_accessor.hh"
#include "db/view/view_builder.hh"
#include "db/view/node_view_update_backlog.hh"
#include "distributed_loader.hh"
// TODO: remove (#293)
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"
#include "gms/feature_service.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/sstables-format-selector.hh"
#include "repair/row_level.hh"
#include "utils/cross-shard-barrier.hh"
#include "debug.hh"
#include "db/schema_tables.hh"
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

    db_config->flush_schema_tables_after_modification.set(false);
}

cql_test_config::cql_test_config(const cql_test_config&) = default;
cql_test_config::~cql_test_config() = default;

static const sstring testing_superuser = "tester";

// END TODO

class single_node_cql_env : public cql_test_env {
public:
    static constexpr std::string_view ks_name = "ks";
    static std::atomic<bool> active;
private:
    sharded<database>& _db;
    sharded<cql3::query_processor>& _qp;
    sharded<auth::service>& _auth_service;
    sharded<db::view::view_builder>& _view_builder;
    sharded<db::view::view_update_generator>& _view_update_generator;
    sharded<service::migration_notifier>& _mnotifier;
    sharded<qos::service_level_controller>& _sl_controller;
    sharded<service::migration_manager>& _mm;
    sharded<db::batchlog_manager>& _batchlog_manager;
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
        if (_db.local().has_keyspace(ks_name)) {
            _core_local.local().client_state.set_keyspace(_db.local(), ks_name);
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
    single_node_cql_env(
            sharded<database>& db,
            sharded<cql3::query_processor>& qp,
            sharded<auth::service>& auth_service,
            sharded<db::view::view_builder>& view_builder,
            sharded<db::view::view_update_generator>& view_update_generator,
            sharded<service::migration_notifier>& mnotifier,
            sharded<service::migration_manager>& mm,
            sharded<qos::service_level_controller> &sl_controller,
            sharded<db::batchlog_manager>& batchlog_manager)
            : _db(db)
            , _qp(qp)
            , _auth_service(auth_service)
            , _view_builder(view_builder)
            , _view_update_generator(view_update_generator)
            , _mnotifier(mnotifier)
            , _sl_controller(sl_controller)
            , _mm(mm)
            , _batchlog_manager(batchlog_manager)
    {
        adjust_rlimit();
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(sstring_view text) override {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        auto qo = make_shared<cql3::query_options>(cql3::query_options::DEFAULT);
        return local_qp().execute_direct(text, *qs, *qo).finally([qs, qo] {});
    }

    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute_cql(
        sstring_view text,
        std::unique_ptr<cql3::query_options> qo) override
    {
        testlog.trace("{}(\"{}\")", __FUNCTION__, text);
        auto qs = make_query_state();
        auto& lqo = *qo;
        return local_qp().execute_direct(text, *qs, lqo).finally([qs, qo = std::move(qo)] {});
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
        std::vector<cql3::raw_value> values,
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
        return local_qp().execute_prepared(std::move(prepared), std::move(id), *qs, lqo, true)
            .finally([qs, qo = std::move(qo)] {});
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

        return modif_stmt->get_mutations(local_qp().proxy(), qo, timeout, false, qo.get_timestamp(*qs), *qs)
            .finally([qs, modif_stmt = std::move(modif_stmt)] {});
    }

    virtual future<> create_table(std::function<schema(std::string_view)> schema_maker) override {
        auto id = utils::UUID_gen::get_time_UUID();
        schema_builder builder(make_lw_shared<schema>(schema_maker(ks_name)));
        builder.set_uuid(id);
        auto s = builder.build(schema_builder::compact_storage::no);
        return _mm.local().announce_new_column_family(s);
    }

    virtual future<> require_keyspace_exists(const sstring& ks_name) override {
        auto& db = _db.local();
        assert(db.has_keyspace(ks_name));
        return make_ready_future<>();
    }

    virtual future<> require_table_exists(const sstring& ks_name, const sstring& table_name) override {
        auto& db = _db.local();
        assert(db.has_schema(ks_name, table_name));
        return make_ready_future<>();
    }

    virtual future<> require_table_exists(std::string_view qualified_name) override {
        auto dot_pos = qualified_name.find_first_of('.');
        assert(dot_pos != std::string_view::npos && dot_pos != 0 && dot_pos != qualified_name.size() - 1);
        assert(_db.local().has_schema(qualified_name.substr(0, dot_pos), qualified_name.substr(dot_pos + 1)));
        return make_ready_future<>();
    }

    virtual future<> require_table_does_not_exist(const sstring& ks_name, const sstring& table_name) override {
        auto& db = _db.local();
        assert(!db.has_schema(ks_name, table_name));
        return make_ready_future<>();
    }

    virtual future<> require_column_has_value(const sstring& table_name,
                                      std::vector<data_value> pk,
                                      std::vector<data_value> ck,
                                      const sstring& column_name,
                                      data_value expected) override {
        auto& db = _db.local();
        auto& cf = db.find_column_family(ks_name, table_name);
        auto schema = cf.schema();
        auto pkey = partition_key::from_deeply_exploded(*schema, pk);
        auto ckey = clustering_key::from_deeply_exploded(*schema, ck);
        auto exp = expected.type()->decompose(expected);
        auto dk = dht::decorate_key(*schema, pkey);
        auto shard = dht::shard_of(*schema, dk._token);
        return _db.invoke_on(shard, [pkey = std::move(pkey),
                                      ckey = std::move(ckey),
                                      ks_name = std::move(ks_name),
                                      column_name = std::move(column_name),
                                      exp = std::move(exp),
                                      table_name = std::move(table_name)] (database& db) mutable {
          auto& cf = db.find_column_family(ks_name, table_name);
          auto schema = cf.schema();
          auto permit = db.get_reader_concurrency_semaphore().make_tracking_only_permit(schema.get(), "require_column_has_value()", db::no_timeout);
          return cf.find_partition_slow(schema, permit, pkey)
                  .then([schema, ckey, column_name, exp] (column_family::const_mutation_partition_ptr p) {
            assert(p != nullptr);
            auto row = p->find_row(*schema, ckey);
            assert(row != nullptr);
            auto col_def = schema->get_column_definition(utf8_type->decompose(column_name));
            assert(col_def != nullptr);
            const atomic_cell_or_collection* cell = row->find_cell(col_def->id);
            if (!cell) {
                assert(((void)"column not set", 0));
            }
            bytes actual;
            if (!col_def->type->is_multi_cell()) {
                auto c = cell->as_atomic_cell(*col_def);
                assert(c.is_live());
                actual = c.value().linearize();
            } else {
                actual = linearized(serialize_for_cql(*col_def->type,
                        cell->as_collection_mutation(), cql_serialization_format::internal()));
            }
            assert(col_def->type->equal(actual, exp));
          });
        });
    }

    virtual service::client_state& local_client_state() override {
        return _core_local.local().client_state;
    }

    virtual database& local_db() override {
        return _db.local();
    }

    cql3::query_processor& local_qp() override {
        return _qp.local();
    }

    sharded<database>& db() override {
        return _db;
    }

    distributed<cql3::query_processor>& qp() override {
        return _qp;
    }

    auth::service& local_auth_service() override {
        return _auth_service.local();
    }

    virtual db::view::view_builder& local_view_builder() override {
        return _view_builder.local();
    }

    virtual db::view::view_update_generator& local_view_update_generator() override {
        return _view_update_generator.local();
    }

    virtual service::migration_notifier& local_mnotifier() override {
        return _mnotifier.local();
    }

    virtual sharded<service::migration_manager>& migration_manager() override {
        return _mm;
    }

    virtual sharded<db::batchlog_manager>& batchlog_manager() override {
        return _batchlog_manager;
    }

    virtual future<> refresh_client_state() override {
        return _core_local.invoke_on_all([] (core_local_state& state) {
            return state.client_state.maybe_update_per_service_level_params();
        });
    }

    future<> start() {
        return _core_local.start(std::ref(_auth_service), std::ref(_sl_controller));
    }

    future<> stop() override {
        return _core_local.stop();
    }

    future<> create_keyspace(std::string_view name) {
        auto query = format("create keyspace {} with replication = {{ 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' : 1 }};", name);
        return execute_cql(query).discard_result();
    }

    static future<> do_with(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in) {
        using namespace std::filesystem;

        return seastar::async([cfg_in = std::move(cfg_in), func] {
            // disable reactor stall detection during startup
            auto blocked_reactor_notify_ms = engine().get_blocked_reactor_notify_ms();
            smp::invoke_on_all([] {
                engine().update_blocked_reactor_notify_ms(std::chrono::milliseconds(1000000));
            }).get();

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

            auto cfg = cfg_in.db_config;
            tmpdir data_dir;
            auto data_dir_path = data_dir.path().string();
            if (!cfg->data_file_directories.is_set()) {
                cfg->data_file_directories.set({data_dir_path});
            } else {
                data_dir_path = cfg->data_file_directories()[0];
            }
            cfg->commitlog_directory.set(data_dir_path + "/commitlog.dir");
            cfg->hints_directory.set(data_dir_path + "/hints.dir");
            cfg->view_hints_directory.set(data_dir_path + "/view_hints.dir");
            cfg->num_tokens.set(256);
            cfg->ring_delay_ms.set(500);
            cfg->shutdown_announce_in_ms.set(0);
            cfg->broadcast_to_all_shards().get();
            create_directories((data_dir_path + "/system").c_str());
            create_directories(cfg->commitlog_directory().c_str());
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

            set_abort_on_internal_error(true);
            const gms::inet_address listen("127.0.0.1");
            constexpr int listen_port = 7000;

            service::services_controller sctl;
            service::sharded_service_ctl<abort_source> abort_source_ctl(sctl, "abort_source", service::default_start_tag{});

            gms::feature_config fcfg = gms::feature_config_from_db_config(*cfg, cfg_in.disabled_features);
            service::sharded_service_ctl<gms::feature_service> feature_service_ctl(sctl, "feature_service");
            feature_service_ctl.start_func = [&fcfg] (sharded<gms::feature_service>& fs) {
                return fs.start(fcfg);
            };
            feature_service_ctl.serve_func = [] (sharded<gms::feature_service>& feature_service) {
                return feature_service.invoke_on_all([] (auto& fs) {
                    fs.enable(fs.known_feature_set());
                });
            };

            service::sharded_service_ctl<locator::shared_token_metadata> token_metadata_ctl(sctl, "token_metadata");
            token_metadata_ctl.start_func = [] (sharded<locator::shared_token_metadata>& tm) {
                return tm.start([] () noexcept { return db::schema_tables::hold_merge_lock(); });
            };

            service::sharded_service_ctl<locator::effective_replication_map_factory> erm_factory_ctl(sctl, "effective_replication_map_factory", service::default_start_tag{});

            service::sharded_service_ctl<service::migration_notifier> mm_notif_ctl(sctl, "migration_notifier", service::default_start_tag{});

            service::service_ctl snitch_ctl(sctl, "snitch",
                [] { return locator::i_endpoint_snitch::create_snitch("SimpleSnitch"); },
                [] { return locator::i_endpoint_snitch::stop_snitch(); });

            service::sharded_service_ctl<netw::messaging_service> messaging_ctl(sctl, "messaging", [listen, listen_port] (sharded<netw::messaging_service>& ms) {
                return ms.start(listen, listen_port);
            });
            messaging_ctl.depends_on(snitch_ctl);

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
            service::sharded_service_ctl<gms::gossiper> gossiper_ctl(sctl, "gossiper");
            gossiper_ctl.start_func = [cfg = std::ref(*cfg), &gcfg,
                abort_source = gossiper_ctl.lookup_dep(abort_source_ctl),
                feature_service = gossiper_ctl.lookup_dep(feature_service_ctl),
                token_metadata = gossiper_ctl.lookup_dep(token_metadata_ctl),
                messaging = gossiper_ctl.lookup_dep(messaging_ctl)
            ] (sharded<gms::gossiper>& gossiper) -> future<> {
                co_await gossiper.start(abort_source, feature_service, token_metadata, messaging, cfg, gcfg);
                // FIXME: until we deglobalize the gossiper
                gms::set_the_gossiper(&gossiper);
            };
            gossiper_ctl.serve_func = [] (sharded<gms::gossiper>& gossiper) {
                return gossiper.invoke_on_all(&gms::gossiper::start);
            };
            gossiper_ctl.shutdown_func = [] (sharded<gms::gossiper>& gossiper) -> future<> {
                co_await gossiper.invoke_on_all(&gms::gossiper::stop);
                // FIXME: until we deglobalize the gossiper
                gms::set_the_gossiper(nullptr);
            };

            service::sharded_service_ctl<semaphore> sst_dir_semaphore_ctl(sctl, "sst_dir_semaphore", [&cfg] (sharded<semaphore>& sst_dir_semaphore) {
                return sst_dir_semaphore.start(cfg->initial_sstable_loading_concurrency());
            });

            database_config dbcfg;
            if (cfg_in.dbcfg) {
                dbcfg = std::move(*cfg_in.dbcfg);
            } else {
                dbcfg.available_memory = memory::stats().total_memory();
            }

            auto scheduling_groups = get_scheduling_groups().get();
            dbcfg.compaction_scheduling_group = scheduling_groups.compaction_scheduling_group;
            dbcfg.memory_compaction_scheduling_group = scheduling_groups.memory_compaction_scheduling_group;
            dbcfg.streaming_scheduling_group = scheduling_groups.streaming_scheduling_group;
            dbcfg.statement_scheduling_group = scheduling_groups.statement_scheduling_group;
            dbcfg.memtable_scheduling_group = scheduling_groups.memtable_scheduling_group;
            dbcfg.memtable_to_cache_scheduling_group = scheduling_groups.memtable_to_cache_scheduling_group;
            dbcfg.gossip_scheduling_group = scheduling_groups.gossip_scheduling_group;
            dbcfg.sstables_format = cfg->enable_sstables_md_format() ? sstables::sstable_version_types::md : sstables::sstable_version_types::mc;

            service::sharded_service_ctl<database> db_ctl(sctl, "database");
            db_ctl.start_func = [cfg = std::ref(*cfg), &dbcfg,
                mm_notif = db_ctl.lookup_dep(mm_notif_ctl),
                feature_service = db_ctl.lookup_dep(feature_service_ctl),
                token_metadata = db_ctl.lookup_dep(token_metadata_ctl),
                abort_source = db_ctl.lookup_dep(abort_source_ctl),
                sst_dir_semaphore = db_ctl.lookup_dep(sst_dir_semaphore_ctl),
                barrier = utils::cross_shard_barrier()
            ] (sharded<database>& db) -> future<> {
                co_await db.start(cfg, dbcfg, mm_notif, feature_service, token_metadata, abort_source, sst_dir_semaphore, barrier);
                debug::the_database = &db;
                co_await db.invoke_on_all(&database::start);
            };
            db_ctl.shutdown_func = [] (sharded<database>& db) {
                return db.invoke_on_all(&database::shutdown);
            };
            db_ctl.stop_func = [] (sharded<database>& db) -> future<> {
                co_await db.stop();
                debug::the_database = nullptr;
            };

            service::storage_proxy::config spcfg {
                .hints_directory_initializer = db::hints::directory_initializer::make_dummy(),
                .available_memory = memory::stats().total_memory(),
            };
            scheduling_group_key_config sg_conf =
                    make_scheduling_group_key_config<service::storage_proxy_stats::stats>();
            scheduling_group_key stats_key = scheduling_group_key_create(sg_conf).get0();
            db::view::node_update_backlog b(smp::count, 10ms);
            service::sharded_service_ctl<service::storage_proxy> sp_ctl(sctl, "storage_proxy");
            sp_ctl.start_func = [&,
                b = std::ref(b),
                db = sp_ctl.lookup_dep(db_ctl),
                gossiper = sp_ctl.lookup_dep(gossiper_ctl),
                feature_service = sp_ctl.lookup_dep(feature_service_ctl),
                token_metadata = sp_ctl.lookup_dep(token_metadata_ctl),
                erm_factory = sp_ctl.lookup_dep(erm_factory_ctl),
                ms = sp_ctl.lookup_dep(messaging_ctl)
            ] (sharded<service::storage_proxy>& sp) -> future<> {
                co_await sp.start(db, gossiper, spcfg, b, stats_key, feature_service, token_metadata, erm_factory, ms);
                // FIXME: until we deglobalize the storage_proxy
                service::set_the_storage_proxy(&sp);
            };
            sp_ctl.stop_func = [] (sharded<service::storage_proxy>& sp) -> future<> {
                co_await sp.stop();
                // FIXME: until we deglobalize the storage_proxy
                service::set_the_storage_proxy(nullptr);
            };

            service::sharded_service_ctl<service::migration_manager> mm_ctl(sctl, "migration_manager");
            mm_ctl.start_func = [
                mm_notif = mm_ctl.lookup_dep(mm_notif_ctl),
                feature_service = mm_ctl.lookup_dep(feature_service_ctl),
                ms = mm_ctl.lookup_dep(messaging_ctl),
                gossiper = mm_ctl.lookup_dep(gossiper_ctl)
            ] (sharded<service::migration_manager>& mm) {
                return mm.start(mm_notif, feature_service, ms, gossiper);
            };
            mm_ctl.depends_on(sp_ctl);  // migration_manager::have_schema_agreement depends on get_local_storage_proxy()

            service::sharded_service_ctl<cql3::cql_config> cql_config_ctl(sctl, "cql_config", [] (sharded<cql3::cql_config>& cql_config) {
                return cql_config.start(cql3::cql_config::default_tag{});
            });

            cql3::query_processor::memory_config qp_mcfg = {memory::stats().total_memory() / 256, memory::stats().total_memory() / 2560};
            service::sharded_service_ctl<cql3::query_processor> qp_ctl(sctl, "cql3_query_processor");
            qp_ctl.start_func = [&qp_mcfg,
                proxy = qp_ctl.lookup_dep(sp_ctl),
                db = qp_ctl.lookup_dep(db_ctl),
                mm_notif = qp_ctl.lookup_dep(mm_notif_ctl),
                mm = qp_ctl.lookup_dep(mm_ctl),
                cql_config = qp_ctl.lookup_dep(cql_config_ctl)
            ] (sharded<cql3::query_processor>& qp) {
                return qp.start(proxy, db, mm_notif, mm, qp_mcfg, cql_config);
            };

            db::batchlog_manager_config bmcfg;
            bmcfg.replay_rate = 100000000;
            bmcfg.write_request_timeout = 2s;
            service::sharded_service_ctl<db::batchlog_manager> bm_ctl(sctl, "batchlog_manager");
            bm_ctl.start_func = [&bmcfg, qp = bm_ctl.lookup_dep(qp_ctl)] (sharded<db::batchlog_manager>& bm) -> future<> {
                co_await bm.start(qp, bmcfg);
            };

            service::sharded_service_ctl<db::system_distributed_keyspace> sys_dist_ks_ctl(sctl, "system_distributed_keyspace");
            sys_dist_ks_ctl.start_func = [
                qp = sys_dist_ks_ctl.lookup_dep(qp_ctl),
                mm = sys_dist_ks_ctl.lookup_dep(mm_ctl),
                proxy = sys_dist_ks_ctl.lookup_dep(sp_ctl)
            ] (sharded<db::system_distributed_keyspace>& sys_dist_ks) {
                return sys_dist_ks.start(qp, mm, proxy);
            };

            cdc::generation_service::config cdc_config;
            cdc_config.ignore_msb_bits = cfg->murmur3_partitioner_ignore_msb_bits();
            /*
             * Currently used when choosing the timestamp of the first CDC stream generation:
             * normally we choose a timestamp in the future so other nodes have a chance to learn about it
             * before it starts operating, but in the single-node-cluster case this is not necessary
             * and would only slow down tests (by having them wait).
             */
            cdc_config.ring_delay = std::chrono::milliseconds(0);
            service::sharded_service_ctl<cdc::generation_service> cdc_generation_service_ctl(sctl, "cdc::generation_service");
            cdc_generation_service_ctl.start_func = [&cdc_config,
                gossiper = cdc_generation_service_ctl.lookup_dep(gossiper_ctl),
                sys_dist_ks = cdc_generation_service_ctl.lookup_dep(sys_dist_ks_ctl),
                abort_source = cdc_generation_service_ctl.lookup_dep(abort_source_ctl),
                token_metadata = cdc_generation_service_ctl.lookup_dep(token_metadata_ctl),
                feature_service = cdc_generation_service_ctl.lookup_dep(feature_service_ctl),
                db = cdc_generation_service_ctl.lookup_dep(db_ctl)
            ] (sharded<cdc::generation_service>& cdc_generation_service) {
                return cdc_generation_service.start(cdc_config, gossiper, sys_dist_ks, abort_source, token_metadata, feature_service, db);
            };

            service::sharded_service_ctl<cdc::cdc_service> cdc_ctl(sctl, "cdc");
            cdc_ctl.start_func = [
                cdc_generation_service = cdc_ctl.lookup_dep(cdc_generation_service_ctl),
                proxy = cdc_ctl.lookup_dep(sp_ctl),
                mm_notif = cdc_ctl.lookup_dep(mm_notif_ctl)
            ] (sharded<cdc::cdc_service>& cdc) {
                auto get_cdc_metadata = [] (cdc::generation_service& svc) { return std::ref(svc.get_cdc_metadata()); };
                return cdc.start(proxy, sharded_parameter(get_cdc_metadata, cdc_generation_service), mm_notif);
            };

            service::sharded_service_ctl<service::raft_group_registry> raft_gr_ctl(sctl, "raft_group_registry");
            raft_gr_ctl.start_func = [
                ms = raft_gr_ctl.lookup_dep(messaging_ctl),
                gossiper = raft_gr_ctl.lookup_dep(gossiper_ctl),
                qp = raft_gr_ctl.lookup_dep(qp_ctl)
            ] (sharded<service::raft_group_registry>& raft_gr) {
                return raft_gr.start(ms, gossiper, qp);
            };
            // We need to have a system keyspace started and
            // initialized to initialize Raft service.
            raft_gr_ctl.depends_on(sys_dist_ks_ctl);

            const bool raft_enabled = cfg->check_experimental(db::experimental_features_t::RAFT);
            if (raft_enabled) {
                raft_gr_ctl.serve_func = [] (sharded<service::raft_group_registry>& raft_gr) {
                    return raft_gr.invoke_on_all(&service::raft_group_registry::init);
                };
                raft_gr_ctl.drain_func = [] (sharded<service::raft_group_registry>& raft_gr, service::on_shutdown) {
                    return raft_gr.invoke_on_all(&service::raft_group_registry::uninit);
                };
            }

            service::sharded_service_ctl<service::endpoint_lifecycle_notifier> elc_notif_ctl(sctl, "endpoint_lifecycle_notifier", service::default_start_tag{});

            service::sharded_service_ctl<db::view::view_update_generator> view_update_generator_ctl(sctl, "view_update_generator");
            view_update_generator_ctl.start_func = [db = view_update_generator_ctl.lookup_dep(db_ctl)] (sharded<db::view::view_update_generator>& view_update_generator) {
                return view_update_generator.start(db);
            };
            view_update_generator_ctl.serve_func = [] (sharded<db::view::view_update_generator>& view_update_generator) {
                return view_update_generator.invoke_on_all(&db::view::view_update_generator::start);
            };
            view_update_generator_ctl.shutdown_func = [] (sharded<db::view::view_update_generator>& view_update_generator) {
                return view_update_generator.invoke_on_all(&db::view::view_update_generator::shutdown);
            };

            sharded<repair_service> repair;

            service::storage_service_config sscfg;
            sscfg.available_memory = memory::stats().total_memory();
            service::sharded_service_ctl<service::storage_service> ss_ctl(sctl, "storage_service");
            ss_ctl.start_func = [sscfg = std::move(sscfg),
                abort_source = ss_ctl.lookup_dep(abort_source_ctl),
                db = ss_ctl.lookup_dep(db_ctl),
                gossiper = ss_ctl.lookup_dep(gossiper_ctl),
                sys_dist_ks = ss_ctl.lookup_dep(sys_dist_ks_ctl),
                feature_service = ss_ctl.lookup_dep(feature_service_ctl),
                mm = ss_ctl.lookup_dep(mm_ctl),
                token_metadata = ss_ctl.lookup_dep(token_metadata_ctl),
                erm_factory = ss_ctl.lookup_dep(erm_factory_ctl),
                messaging = ss_ctl.lookup_dep(messaging_ctl),
                cdc_generation_service = ss_ctl.lookup_dep(cdc_generation_service_ctl),
                repair = std::ref(repair),
                raft_gr = ss_ctl.lookup_dep(raft_gr_ctl),
                elc_notif = ss_ctl.lookup_dep(elc_notif_ctl),
                bm = ss_ctl.lookup_dep(bm_ctl)
            ] (sharded<service::storage_service>& ss) {
                return ss.start(abort_source, db, gossiper, sys_dist_ks, feature_service, sscfg,
                        mm, token_metadata, erm_factory, messaging, cdc_generation_service,
                        repair, raft_gr, elc_notif, bm);
            };

            service::sharded_service_ctl<db::system_keyspace> system_keyspace_ctl(sctl, "system_keyspace");
            system_keyspace_ctl.start_func = [qp = system_keyspace_ctl.lookup_dep(qp_ctl)] (sharded<db::system_keyspace>&) {
                // In main.cc we call db::system_keyspace::setup which calls
                // minimal_setup and init_local_cache
                // FIXME: until we deglobalize the system_keyspace
                db::system_keyspace::minimal_setup(qp);
                return db::system_keyspace::init_local_cache();
            };
            system_keyspace_ctl.stop_func = [] (sharded<db::system_keyspace>&) {
                db::qctx = {};
                return db::system_keyspace::deinit_local_cache();
            };

            service::sharded_service_ctl<distributed_loader> distributed_loader_ctl(sctl, "distributed_loader", service::default_start_tag{});
            distributed_loader_ctl.serve_func = [cfg = std::ref(*cfg),
                    &db = distributed_loader_ctl.lookup_dep(db_ctl).get(),
                    &ss = distributed_loader_ctl.lookup_dep(ss_ctl).get(),
                    &proxy = distributed_loader_ctl.lookup_dep(sp_ctl).get()] (sharded<distributed_loader>&) -> future<> {
                co_await distributed_loader::init_system_keyspace(db, ss, cfg);

                auto& ks = db.local().find_keyspace(db::system_keyspace::NAME);
                co_await parallel_for_each(ks.metadata()->cf_meta_data(), [&ks] (auto& pair) {
                    auto cfm = pair.second;
                    return ks.make_directory_for_column_family(cfm->cf_name(), cfm->id());
                });

                co_await distributed_loader::init_non_system_keyspaces(db, proxy);

                co_await db.invoke_on_all([] (database& db) {
                    for (auto& x : db.get_column_families()) {
                        table& t = *(x.second);
                        t.enable_auto_compaction();
                    }
                });
            };

            auth::permissions_cache_config perm_cache_config;
            perm_cache_config.max_entries = cfg->permissions_cache_max_entries();
            perm_cache_config.validity_period = std::chrono::milliseconds(cfg->permissions_validity_in_ms());
            perm_cache_config.update_period = std::chrono::milliseconds(cfg->permissions_update_interval_in_ms());

            const qualified_name qualified_authorizer_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authorizer());
            const qualified_name qualified_authenticator_name(auth::meta::AUTH_PACKAGE_NAME, cfg->authenticator());
            const qualified_name qualified_role_manager_name(auth::meta::AUTH_PACKAGE_NAME, cfg->role_manager());

            auth::service_config auth_config;
            auth_config.authorizer_java_name = qualified_authorizer_name;
            auth_config.authenticator_java_name = qualified_authenticator_name;
            auth_config.role_manager_java_name = qualified_role_manager_name;

            service::sharded_service_ctl<auth::service> auth_service_ctl(sctl, "auth_service_ctl");
            auth_service_ctl.start_func = [
                &perm_cache_config,
                &auth_config,
                qp = auth_service_ctl.lookup_dep(qp_ctl),
                mm_notif = auth_service_ctl.lookup_dep(mm_notif_ctl),
                mm = auth_service_ctl.lookup_dep(mm_ctl)
            ] (sharded<auth::service>& auth_service) {
                return auth_service.start(perm_cache_config, qp, mm_notif, mm, auth_config);
            };
            auth_service_ctl.serve_func = [
                    dl = auth_service_ctl.lookup_dep(distributed_loader_ctl),
                    &mm = mm_ctl.service()] (sharded<auth::service>& auth_service) {
                return auth_service.invoke_on_all([&mm] (auth::service& auth) {
                    return auth.start(mm.local());
                });
            };
            auth_service_ctl.shutdown_func = [] (sharded<auth::service>& auth_service) {
                return auth_service.invoke_on_all(&auth::service::stop);
            };

            auto slc_ctl = service::sharded_service_ctl<qos::service_level_controller>(sctl, "service_level_controller");
            slc_ctl.start_func = [auth_service = slc_ctl.lookup_dep(auth_service_ctl)] (sharded<qos::service_level_controller>& sl_controller) -> future<> {
                co_await sl_controller.start(auth_service, qos::service_level_options{});
                co_await sl_controller.invoke_on_all(&qos::service_level_controller::start);
            };
            slc_ctl.serve_func = [&sys_dist_ks = slc_ctl.lookup_dep(sys_dist_ks_ctl).get()] (sharded<qos::service_level_controller>& sl_controller) {
                return sl_controller.invoke_on_all([&sl_controller, &sys_dist_ks] (qos::service_level_controller& service) {
                    qos::service_level_controller::service_level_distributed_data_accessor_ptr service_level_data_accessor =
                        ::static_pointer_cast<qos::service_level_controller::service_level_distributed_data_accessor>(
                                make_shared<qos::unit_test_service_levels_accessor>(sl_controller, sys_dist_ks));
                    return service.set_distributed_data_accessor(std::move(service_level_data_accessor));
                });
            };

            service::sharded_service_ctl<db::view::view_builder> view_builder_ctl(sctl, "view_builder");
            view_builder_ctl.start_func = [
                db = view_builder_ctl.lookup_dep(db_ctl),
                sys_dist_ks = view_builder_ctl.lookup_dep(sys_dist_ks_ctl),
                mm_notif = view_builder_ctl.lookup_dep(mm_notif_ctl),
                &mm = view_builder_ctl.lookup_dep(mm_ctl).get()
            ] (sharded<db::view::view_builder>& view_builder) -> future<> {
                co_await view_builder.start(db, sys_dist_ks, mm_notif);
                co_await view_builder.invoke_on_all([&mm] (db::view::view_builder& vb) {
                    return vb.start(mm.local());
                });
            };
            view_builder_ctl.drain_func = [] (sharded<db::view::view_builder>& view_builder, service::on_shutdown) {
                return view_builder.invoke_on_all(&db::view::view_builder::drain);
            };

            service::sharded_service_ctl<single_node_cql_env> env_ctl(sctl, "single_node_cql_env");
            env_ctl.start_func = [
                db = env_ctl.lookup_dep(db_ctl),
                qp = env_ctl.lookup_dep(qp_ctl),
                auth_service = env_ctl.lookup_dep(auth_service_ctl),
                view_builder = env_ctl.lookup_dep(view_builder_ctl),
                view_update_generator = env_ctl.lookup_dep(view_update_generator_ctl),
                mm_notif = env_ctl.lookup_dep(mm_notif_ctl),
                mm = env_ctl.lookup_dep(mm_ctl),
                sl_controller = env_ctl.lookup_dep(slc_ctl),
                bm = env_ctl.lookup_dep(bm_ctl)
            ] (sharded<single_node_cql_env>& env) -> future<> {
                co_await env.start_single(db, qp, auth_service, view_builder, view_update_generator, mm_notif, mm, sl_controller, bm);
                co_await env.local().start();
            };
            env_ctl.serve_func = [
                    dl = env_ctl.lookup_dep(distributed_loader_ctl),
                    &auth_service = env_ctl.lookup_dep(auth_service_ctl).get(),
                    &ss = env_ctl.lookup_dep(ss_ctl).get(),
                    &mm = env_ctl.lookup_dep(mm_ctl).get(),
                    &sl_controller = env_ctl.lookup_dep(slc_ctl).get()] (sharded<single_node_cql_env>& sharded_env) -> future<> {
                testlog.debug("single_node_cql_env: in storage_server");
                co_await ss.local().init_server();
                testlog.debug("single_node_cql_env: joining cluster");
                co_await ss.local().join_cluster();

                testlog.debug("single_node_cql_env: starting service_level_controller");
                co_await sl_controller.invoke_on_all(&qos::service_level_controller::start);
            };

            sctl.start().get();
            auto stop_sctl = deferred_stop(sctl);

            smp::invoke_on_all([blocked_reactor_notify_ms] {
                engine().update_blocked_reactor_notify_ms(blocked_reactor_notify_ms);
            }).get();

            sctl.serve().get();

            // Create the testing user.
            testlog.debug("single_node_cql_env: creating test user");
            try {
                auth::role_config config;
                config.is_superuser = true;
                config.can_login = true;

                auth::create_role(
                        auth_service_ctl.local(),
                        testing_superuser,
                        config,
                        auth::authentication_options()).get0();
            } catch (const auth::role_already_exists&) {
                // The default user may already exist if this `cql_test_env` is starting with previously populated data.
            }

            auto& env = env_ctl.local();
            if (!env.local_db().has_keyspace(ks_name)) {
                testlog.debug("single_node_cql_env: creating keyspace {}", ks_name);
                env.create_keyspace(ks_name).get();
            }

            testlog.debug("single_node_cql_env: running test function");
            with_scheduling_group(dbcfg.statement_scheduling_group, [&func, &env] {
                return func(env);
            }).get();
        });
    }

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
        return local_qp().execute_batch(batch, *qs, lqo, {}).finally([qs, batch, qo = std::move(qo)] {});
    }
};

std::atomic<bool> single_node_cql_env::active = { false };

future<> do_with_cql_env(std::function<future<>(cql_test_env&)> func, cql_test_config cfg_in) {
    return single_node_cql_env::do_with(func, std::move(cfg_in));
}

future<> do_with_cql_env_thread(std::function<void(cql_test_env&)> func, cql_test_config cfg_in, thread_attributes thread_attr) {
    return single_node_cql_env::do_with([func = std::move(func), thread_attr] (auto& e) {
        return seastar::async(thread_attr, [func = std::move(func), &e] {
            return func(e);
        });
    }, std::move(cfg_in));
}

reader_permit make_reader_permit(cql_test_env& env) {
    return env.local_db().get_reader_concurrency_semaphore().make_tracking_only_permit(nullptr, "test", db::no_timeout);
}

cql_test_config raft_cql_test_config() {
    cql_test_config c;
    c.db_config->experimental_features({db::experimental_features_t::RAFT});
    return c;
}

namespace debug {

seastar::sharded<database>* the_database;

}
