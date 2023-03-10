/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "test/lib/scylla_tests_cmdline_options.hh"
#include "test/lib/test_services.hh"
#include "test/lib/sstable_test_env.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "dht/i_partitioner.hh"
#include "gms/feature_service.hh"
#include "repair/row_level.hh"
#include "replica/compaction_group.hh"
#include <boost/program_options.hpp>
#include <iostream>
#include <seastar/util/defer.hh>

static const sstring some_keyspace("ks");
static const sstring some_column_family("cf");

table_for_tests::data::data()
    : semaphore(reader_concurrency_semaphore::no_limits{}, "table_for_tests")
{ }

table_for_tests::data::~data() {}

schema_ptr table_for_tests::make_default_schema() {
    return schema_builder(some_keyspace, some_column_family)
        .with_column(utf8_type->decompose("p1"), utf8_type, column_kind::partition_key)
        .build();
}

table_for_tests::table_for_tests(sstables::sstables_manager& sstables_manager)
    : table_for_tests(
        sstables_manager,
        make_default_schema()
    )
{ }

class table_for_tests::table_state : public compaction::table_state {
    table_for_tests::data& _data;
    sstables::sstables_manager& _sstables_manager;
    std::vector<sstables::shared_sstable> _compacted_undeleted;
    tombstone_gc_state _tombstone_gc_state;
    mutable compaction_backlog_tracker _backlog_tracker;
private:
    replica::table& table() const noexcept {
        return *_data.cf;
    }
public:
    explicit table_state(table_for_tests::data& data, sstables::sstables_manager& sstables_manager)
            : _data(data)
            , _sstables_manager(sstables_manager)
            , _tombstone_gc_state(nullptr)
            , _backlog_tracker(get_compaction_strategy().make_backlog_tracker())
    {
    }
    const schema_ptr& schema() const noexcept override {
        return table().schema();
    }
    unsigned min_compaction_threshold() const noexcept override {
        return schema()->min_compaction_threshold();
    }
    bool compaction_enforce_min_threshold() const noexcept override {
        return true;
    }
    const sstables::sstable_set& main_sstable_set() const override {
        return table().as_table_state().main_sstable_set();
    }
    const sstables::sstable_set& maintenance_sstable_set() const override {
        return table().as_table_state().maintenance_sstable_set();
    }
    std::unordered_set<sstables::shared_sstable> fully_expired_sstables(const std::vector<sstables::shared_sstable>& sstables, gc_clock::time_point query_time) const override {
        return sstables::get_fully_expired_sstables(*this, sstables, query_time);
    }
    const std::vector<sstables::shared_sstable>& compacted_undeleted_sstables() const noexcept override {
        return _compacted_undeleted;
    }
    sstables::compaction_strategy& get_compaction_strategy() const noexcept override {
        return table().get_compaction_strategy();
    }
    reader_permit make_compaction_reader_permit() const override {
        return _data.semaphore.make_tracking_only_permit(&*schema(), "table_for_tests::table_state", db::no_timeout);
    }
    sstables::sstables_manager& get_sstables_manager() noexcept override {
        return _sstables_manager;
    }
    sstables::shared_sstable make_sstable() const override {
        return table().make_sstable();
    }
    sstables::sstable_writer_config configure_writer(sstring origin) const override {
        return _sstables_manager.configure_writer(std::move(origin));
    }

    api::timestamp_type min_memtable_timestamp() const override {
        return table().min_memtable_timestamp();
    }
    future<> on_compaction_completion(sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) override {
        return table().as_table_state().on_compaction_completion(std::move(desc), offstrategy);
    }
    bool is_auto_compaction_disabled_by_user() const noexcept override {
        return table().is_auto_compaction_disabled_by_user();
    }
    const tombstone_gc_state& get_tombstone_gc_state() const noexcept override {
        return _tombstone_gc_state;
    }
    compaction_backlog_tracker& get_backlog_tracker() override {
        return _backlog_tracker;
    }
};

table_for_tests::table_for_tests(sstables::sstables_manager& sstables_manager, schema_ptr s, std::optional<sstring> datadir, sstables::sstable_version_types sstables_version)
    : _data(make_lw_shared<data>())
{
    _data->s = s ? s : make_default_schema();
    _data->cfg = replica::table::config{.compaction_concurrency_semaphore = &_data->semaphore};
    _data->cfg.enable_disk_writes = bool(datadir);
    _data->cfg.datadir = datadir.value_or(sstring());
    _data->cfg.cf_stats = &_data->cf_stats;
    _data->cfg.enable_commitlog = false;
    _data->cm.enable();
    _data->cf = make_lw_shared<replica::column_family>(_data->s, _data->cfg, replica::column_family::no_commitlog(), _data->cm, sstables_manager, _data->cl_stats, _data->tracker);
    _data->cf->mark_ready_for_writes();
    _data->table_s = std::make_unique<table_state>(*_data, sstables_manager);
    _data->cm.add(*_data->table_s);
    _data->sstables_version = sstables_version;
}

compaction::table_state& table_for_tests::as_table_state() noexcept {
    return *_data->table_s;
}

future<> table_for_tests::stop() {
    auto data = _data;
    co_await data->cm.remove(*data->table_s);
    co_await when_all_succeed(data->cm.stop(), data->semaphore.stop()).discard_result();
}

namespace sstables {

std::unique_ptr<db::config> make_db_config(sstring temp_dir) {
    auto cfg = std::make_unique<db::config>();
    cfg->data_file_directories.set({ temp_dir });
    return cfg;
}

test_env::impl::impl(test_env_config cfg)
    : dir()
    , db_config(make_db_config(dir.path().native()))
    , dir_sem(1)
    , feature_service(gms::feature_config_from_db_config(*db_config))
    , mgr(cfg.large_data_handler == nullptr ? nop_ld_handler : *cfg.large_data_handler, *db_config, feature_service, cache_tracker, memory::stats().total_memory(), dir_sem)
    , semaphore(reader_concurrency_semaphore::no_limits{}, "sstables::test_env")
{ }

}

static std::pair<int, char**> rebuild_arg_list_without(int argc, char** argv, const char* filter_out, bool exclude_positional_arg = false) {
    int new_argc = 0;
    char** new_argv = (char**) malloc(argc * sizeof(char*));
    std::memset(new_argv, 0, argc * sizeof(char*));
    bool exclude_next_arg = false;
    for (auto i = 0; i < argc; i++) {
        if (std::exchange(exclude_next_arg, false)) {
            continue;
        }
        if (strcmp(argv[i], filter_out) == 0) {
            // if arg filtered out has positional arg, that has to be excluded too.
            exclude_next_arg = exclude_positional_arg;
            continue;
        }
        new_argv[new_argc] = (char*) malloc(strlen(argv[i]) + 1);
        std::strcpy(new_argv[new_argc], argv[i]);
        new_argc++;
    }
    return std::make_pair(new_argc, new_argv);
}

static void free_arg_list(int argc, char** argv) {
    for (auto i = 0; i < argc; i++) {
        if (argv[i]) {
            free(argv[i]);
        }
    }
    free(argv);
}

scylla_tests_cmdline_options_processor::~scylla_tests_cmdline_options_processor() {
    if (_new_argv) {
        free_arg_list(_new_argc, _new_argv);
    }
}

std::pair<int, char**> scylla_tests_cmdline_options_processor::process_cmdline_options(int argc, char** argv) {
    namespace po = boost::program_options;

    // Removes -- (intended to separate boost suite args from seastar ones) which confuses boost::program_options.
    auto [new_argc, new_argv] = rebuild_arg_list_without(argc, argv, "--");
    auto _ = defer([argc = new_argc, argv = new_argv] {
        free_arg_list(argc, argv);
    });

    po::options_description desc("Scylla tests additional options");
    desc.add_options()
            ("help", "Produces help message")
            ("x-log2-compaction-groups", po::value<unsigned>()->default_value(0), "Controls static number of compaction groups per table per shard. For X groups, set the option to log (base 2) of X. Example: Value of 3 implies 8 groups.");
    po::variables_map vm;

    po::parsed_options parsed = po::command_line_parser(new_argc, new_argv).
            options(desc).
            allow_unregistered().
            run();

    po::store(parsed, vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return std::make_pair(argc, argv);
    }

    unsigned x_log2_compaction_groups = vm["x-log2-compaction-groups"].as<unsigned>();
    if (x_log2_compaction_groups) {
        std::cout << "Setting x_log2_compaction_groups to " << x_log2_compaction_groups << std::endl;
        replica::set_minimum_x_log2_compaction_groups(x_log2_compaction_groups);
        auto [_new_argc, _new_argv] = rebuild_arg_list_without(argc, argv, "--x-log2-compaction-groups", true);
        return std::make_pair(_new_argc, _new_argv);
    }

    return std::make_pair(argc, argv);
}
