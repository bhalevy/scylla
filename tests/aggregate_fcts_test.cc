/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */


#include <boost/range/irange.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/multiprecision/cpp_int.hpp>

#include "utils/big_decimal.hh"
#include "exceptions/exceptions.hh"
#include "tests/test-utils.hh"
#include "tests/cql_test_env.hh"
#include "tests/cql_assertions.hh"

#include "core/future-util.hh"
#include "transport/messages/result_message.hh"

#include "db/config.hh"

namespace {

void create_table(cql_test_env& e) {
    e.execute_cql("CREATE TABLE test (a tinyint primary key,"
                  " b smallint,"
                  " c int,"
                  " d bigint,"
                  " e float,"
                  " f double,"
                  " g_0 decimal,"
                  " g_2 decimal,"
                  " h varint, "
                  " t text,"
                  " dt date,"
                  " tm timestamp,"
                  " tu timeuuid,"
                  " bl blob)").get();

    e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g_0, g_2, h, t, dt, tm, tu, bl) VALUES (1, 1, 1, 1, 1, 1, 1, 1.00, 1, 'a', '2017-12-02', '2017-12-02t03:00:00', b650cbe0-f914-11e7-8892-000000000004, 0x0101)").get();
    e.execute_cql("INSERT INTO test (a, b, c, d, e, f, g_0, g_2, h, t, dt, tm, tu, bl) VALUES (2, 2, 2, 2, 2, 2, 2, 2.00, 2, 'b', '2016-12-02', '2016-12-02t06:00:00', D2177dD0-EAa2-11de-a572-001B779C76e3, 0x01)").get();
}

} /* anonymous namespace */

SEASTAR_TEST_CASE(test_aggregate_avg) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT avg(a), "
                                 "avg(b), "
                                 "avg(c), "
                                 "avg(d), "
                                 "avg(e), "
                                 "avg(f), "
                                 "avg(g_0), "
                                 "avg(g_2), "
                                 "avg(h) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(1))},
                                                          {short_type->decompose(int16_t(1))},
                                                          {int32_type->decompose(int32_t(1))},
                                                          {long_type->decompose(int64_t(1))},
                                                          {float_type->decompose((1.f+2.f)/2L)},
                                                          {double_type->decompose((1.d+2.d)/2L)},
                                                          {decimal_type->from_string("2")},
                                                          {decimal_type->from_string("1.50")},
                                                          {varint_type->from_string("1")}});
    });
}

SEASTAR_TEST_CASE(test_aggregate_sum) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT sum(a), "
                                 "sum(b), "
                                 "sum(c), "
                                 "sum(d), "
                                 "sum(e), "
                                 "sum(f), "
                                 "sum(g_0), "
                                 "sum(g_2), "
                                 "sum(h) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(3))},
                                                          {short_type->decompose(int16_t(3))},
                                                          {int32_type->decompose(int32_t(3))},
                                                          {long_type->decompose(int64_t(3))},
                                                          {float_type->decompose(3.f)},
                                                          {double_type->decompose(3.d)},
                                                          {decimal_type->from_string("3")},
                                                          {decimal_type->from_string("3.00")},
                                                          {varint_type->from_string("3")}});
    });
}

SEASTAR_TEST_CASE(test_aggregate_max) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT max(a), "
                                 "max(b), "
                                 "max(c), "
                                 "max(d), "
                                 "max(e), "
                                 "max(f), "
                                 "max(g_0), "
                                 "max(g_2), "
                                 "max(h), "
                                 "max(t), "
                                 "max(dt), "
                                 "max(tm), "
                                 "max(tu), "
                                 "max(bl) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(2))},
                                                          {short_type->decompose(int16_t(2))},
                                                          {int32_type->decompose(int32_t(2))},
                                                          {long_type->decompose(int64_t(2))},
                                                          {float_type->decompose(2.f)},
                                                          {double_type->decompose(2.d)},
                                                          {decimal_type->from_string("2")},
                                                          {decimal_type->from_string("2.00")},
                                                          {varint_type->from_string("2")},
                                                          {utf8_type->from_string("b")},
                                                          {simple_date_type->from_string("2017-12-02")},
                                                          {timestamp_type->from_string("2017-12-02t03:00:00")},
                                                          {timeuuid_type->from_string("D2177dD0-EAa2-11de-a572-001B779C76e3")},
                                                          {bytes_type->from_string("0101")},
        });
    });
}

SEASTAR_TEST_CASE(test_aggregate_min) {
    return do_with_cql_env_thread([&] (auto& e) {
        create_table(e);

        auto msg = e.execute_cql("SELECT min(a), "
                                 "min(b), "
                                 "min(c), "
                                 "min(d), "
                                 "min(e), "
                                 "min(f), "
                                 "min(g_0), "
                                 "min(g_2), "
                                 "min(h), "
                                 "min(t), "
                                 "min(dt), "
                                 "min(tm), "
                                 "min(tu), "
                                 "min(bl) FROM test").get0();

        assert_that(msg).is_rows().with_size(1).with_row({{byte_type->decompose(int8_t(1))},
                                                          {short_type->decompose(int16_t(1))},
                                                          {int32_type->decompose(int32_t(1))},
                                                          {long_type->decompose(int64_t(1))},
                                                          {float_type->decompose(1.f)},
                                                          {double_type->decompose(1.d)},
                                                          {decimal_type->from_string("1")},
                                                          {decimal_type->from_string("1.00")},
                                                          {varint_type->from_string("1")},
                                                          {utf8_type->from_string("a")},
                                                          {simple_date_type->from_string("2016-12-02")},
                                                          {timestamp_type->from_string("2016-12-02t06:00:00")},
                                                          {timeuuid_type->from_string("b650cbe0-f914-11e7-8892-000000000004")},
                                                          {bytes_type->from_string("01")},
        });
    });
}

SEASTAR_TEST_CASE(test_aggregate_count) {
    return do_with_cql_env_thread([&] (auto& e) {

        e.execute_cql("CREATE TABLE test(a int primary key, b int, c int, bl blob)").get();
        e.execute_cql("INSERT INTO test(a, b, bl) VALUES (1, 1, 0x01)").get();
        e.execute_cql("INSERT INTO test(a, c) VALUES (2, 2)").get();
        e.execute_cql("INSERT INTO test(a, c, bl) VALUES (3, 3, 0x03)").get();

        {
            auto msg = e.execute_cql("SELECT count(*) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(a) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(b) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(1))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(c) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(2))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(a), count(b), count(c), count(bl), count(*) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))},
                                                              {long_type->decompose(int64_t(1))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(3))}});
        }
        {
            auto msg = e.execute_cql("SELECT count(a), count(b), count(c), count(*) FROM test LIMIT 1").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{long_type->decompose(int64_t(3))},
                                                              {long_type->decompose(int64_t(1))},
                                                              {long_type->decompose(int64_t(2))},
                                                              {long_type->decompose(int64_t(3))}});
        }
    });
}

SEASTAR_TEST_CASE(test_reverse_type_aggregation) {
    return do_with_cql_env_thread([&] (auto& e) {
        e.execute_cql("CREATE TABLE test(p int, c timestamp, v int, primary key (p, c)) with clustering order by (c desc)").get();
        e.execute_cql("INSERT INTO test(p, c, v) VALUES (1, 1, 1)").get();
        e.execute_cql("INSERT INTO test(p, c, v) VALUES (1, 2, 1)").get();

        {
            auto tp = db_clock::from_time_t({ 0 }) + std::chrono::milliseconds(1);
            auto msg = e.execute_cql("SELECT min(c) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{timestamp_type->decompose(tp)}});
        }
        {
            auto tp = db_clock::from_time_t({ 0 }) + std::chrono::milliseconds(2);
            auto msg = e.execute_cql("SELECT max(c) FROM test").get0();
            assert_that(msg).is_rows().with_size(1).with_row({{timestamp_type->decompose(tp)}});
        }
    });
}
