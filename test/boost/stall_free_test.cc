/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <seastar/testing/thread_test_case.hh>
#include "utils/stall_free.hh"
#include "utils/small_vector.hh"
#include "utils/chunked_vector.hh"

SEASTAR_THREAD_TEST_CASE(test_merge1) {
    std::list<int> l1{1, 2, 5, 8};
    std::list<int> l2{3};
    std::list<int> expected{1,2,3,5,8};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge2) {
    std::list<int> l1{1};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{1,3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge3) {
    std::list<int> l1{};
    std::list<int> l2{3, 5, 6};
    std::list<int> expected{3,5,6};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_merge4) {
    std::list<int> l1{1};
    std::list<int> l2{};
    std::list<int> expected{1};
    utils::merge_to_gently(l1, l2, std::less<int>());
    BOOST_CHECK(l1 == expected);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_string) {
    sstring s0 = "hello";
    utils::clear_gently(s0).get();
    BOOST_CHECK(s0.empty());

    std::string s1 = "hello";
    utils::clear_gently(s1).get();
    BOOST_CHECK(s1.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_unique_ptr) {
    std::unique_ptr<int> p = std::make_unique<int>(0);

    utils::clear_gently(p).get();
    BOOST_CHECK(p);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_non_trivial_unique_ptr) {
    struct X {
        int v;
        std::function<void(int)> on_clear;
        X(int i, std::function<void (int)> f) : v(i), on_clear(std::move(f)) {}
        future<> clear_gently() {
            on_clear(v);
            return make_ready_future<>();
        }
    };
    int cleared_gently = 0;
    std::unique_ptr<X> p = std::make_unique<X>(0, [&cleared_gently] (int) {
        cleared_gently++;
    });

    utils::clear_gently(p).get();
    BOOST_CHECK(p);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_shared_ptr) {
    struct X {
        int v;
        std::function<void(int)> on_clear;
        X(int i, std::function<void (int)> f) : v(i), on_clear(std::move(f)) {}
        future<> clear_gently() {
            on_clear(v);
            return make_ready_future<>();
        }
    };
    int cleared_gently = 0;
    lw_shared_ptr<X> p0 = make_lw_shared<X>(0, [&cleared_gently] (int) {
        cleared_gently++;
    });

    utils::clear_gently(p0).get();
    BOOST_CHECK(p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);

    lw_shared_ptr<X> p1 = p0;

    utils::clear_gently(p0).get();
    BOOST_CHECK(p0);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
    utils::clear_gently(p1).get();
    BOOST_CHECK(p1);
    BOOST_REQUIRE_EQUAL(cleared_gently, 1);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_array) {
    std::array<int, 3> a = {0, 1, 2};

    utils::clear_gently(a).get();
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_non_trivial_array) {
    struct X {
        int v;
        std::function<void(int)> on_clear;
        X(int i, std::function<void (int)> f) : v(i), on_clear(std::move(f)) {}
        future<> clear_gently() {
            on_clear(v);
            return make_ready_future<>();
        }
    };
    constexpr int count = 3;
    std::array<std::unique_ptr<X>, count> a;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        a[i] = std::make_unique<X>(i, [&cleared_gently] (int) {
            cleared_gently++;
        });
    }

    utils::clear_gently(a).get();
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_vector) {
    constexpr int count = 100;
    std::vector<int> v;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(i);
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_trivial_small_vector) {
    utils::small_vector<int, 1> v;
    constexpr int count = 10;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(i);
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_vector) {
    static thread_local std::vector<int> res;
    struct X {
        int v;
        X(int i) : v(i) {}
        X(X&& x) noexcept : v(x.v) {}
        future<> clear_gently() noexcept {
            res.push_back(v);
            return make_ready_future<>();
        }
    };
    static_assert(std::is_trivially_destructible_v<X>);
    std::vector<X> v;
    constexpr int count = 100;

    res.reserve(count);
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());

    // verify that the items were cleared in reverse order
    for (int i = 0; i < count; i++) {
        BOOST_REQUIRE_EQUAL(res[i], 99 - i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_small_vector) {
    std::vector<int> res;
    struct X {
        std::unique_ptr<int> v;
        std::function<void (int)> on_clear;
        X(int i, std::function<void (int)> f) : v(std::make_unique<int>(i)), on_clear(std::move(f)) {}
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
        X& operator=(X&& x) noexcept {
            if (&x != this) {
                std::swap(v, x.v);
                std::swap(on_clear, x.on_clear);
            }
            return *this;
        }
        future<> clear_gently() noexcept {
            on_clear(*v);
            v.reset();
            return make_ready_future<>();
        }
    };
    utils::small_vector<X, 1> v;
    constexpr int count = 100;

    res.reserve(count);
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i, [&res] (int i) {
            res.emplace_back(i);
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());

    // verify that the items were cleared in reverse order
    for (int i = 0; i < count; i++) {
        BOOST_REQUIRE_EQUAL(res[i], 99 - i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_chunked_vector) {
    std::vector<int> res;
    struct X {
        std::unique_ptr<int> v;
        std::function<void (int)> on_clear;
        X(int i, std::function<void (int)> f) : v(std::make_unique<int>(i)), on_clear(std::move(f)) {}
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
        future<> clear_gently() noexcept {
            on_clear(*v);
            v.reset();
            return make_ready_future<>();
        }
    };
    utils::chunked_vector<X> v;
    constexpr int count = 100;

    res.reserve(count);
    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i, [&res] (int i) {
            res.emplace_back(i);
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());

    // verify that the items were cleared in reverse order
    for (int i = 0; i < count; i++) {
        BOOST_REQUIRE_EQUAL(res[i], 99 - i);
    }
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_list) {
    struct X {
        std::unique_ptr<int> v;
        std::function<void (int)> on_clear;
        X(int i, std::function<void (int)> f) : v(std::make_unique<int>(i)), on_clear(std::move(f)) {}
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
        future<> clear_gently() noexcept {
            on_clear(*v);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int count = 100;
    std::list<X> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i, [&cleared_gently] (int) {
            cleared_gently++;
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_deque) {
    struct X {
        std::unique_ptr<int> v;
        std::function<void (int)> on_clear;
        X(int i, std::function<void (int)> f) : v(std::make_unique<int>(i)), on_clear(std::move(f)) {}
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
        future<> clear_gently() noexcept {
            on_clear(*v);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int count = 100;
    std::deque<X> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i, [&cleared_gently] (int) {
            cleared_gently++;
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unordered_map) {
    std::unordered_map<int, sstring> c;
    constexpr int count = 100;

    for (int i = 0; i < count; i++) {
        c.insert(std::pair<int, sstring>(i, format("{}", i)));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_vector) {
    struct X {
        std::unique_ptr<std::pair<int, int>> v;
        std::function<void (int, int)> on_clear;
        X(int i, int j, std::function<void (int, int)> f) : v(std::make_unique<std::pair<int, int>>(i, j)), on_clear(std::move(f)) {}
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
        future<> clear_gently() noexcept {
            std::pair<int, int>* p = v.get();
            on_clear(p->first, p->second);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int top_count = 10;
    constexpr int count = 10;
    std::vector<std::vector<X>> c;
    int cleared_gently = 0;

    c.reserve(top_count);
    for (int i = 0; i < top_count; i++) {
        std::vector<X> v;
        v.reserve(count);
        for (int j = 0; j < count; j++) {
            v.emplace_back(X(i, j, [&cleared_gently] (int, int) {
                cleared_gently++;
            }));
        }
        c.emplace_back(std::move(v));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_object) {
    struct X {
        std::unique_ptr<int> v;
        std::function<void (int)> on_clear;
        X(int i, std::function<void (int)> f) : v(std::make_unique<int>(i)), on_clear(std::move(f)) { }
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) { }
        future<> clear_gently() {
            on_clear(*v);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int count = 100;
    std::vector<X> v;
    int cleared_gently = 0;

    v.reserve(count);
    for (int i = 0; i < count; i++) {
        v.emplace_back(X(i, [&cleared_gently] (int) {
            cleared_gently++;
        }));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_map_object) {
    struct X {
        std::unique_ptr<int> v;
        std::function<void (int)> on_clear;
        X(int i, std::function<void (int)> f) : v(std::make_unique<int>(i)), on_clear(std::move(f)) { }
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) { }
        future<> clear_gently() {
            on_clear(*v);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int count = 100;
    std::map<int, X> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.insert(std::pair<int, X>(i, X(i, [&cleared_gently] (int) {
            cleared_gently++;
        })));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_unordered_map_object) {
    struct X {
        std::unique_ptr<int> v;
        std::function<void (int)> on_clear;
        X(int i, std::function<void (int)> f) : v(std::make_unique<int>(i)), on_clear(std::move(f)) { }
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) { }
        future<> clear_gently() {
            on_clear(*v);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int count = 100;
    std::unordered_map<int, X> v;
    int cleared_gently = 0;

    for (int i = 0; i < count; i++) {
        v.insert(std::pair<int, X>(i, X(i, [&cleared_gently] (int) {
            cleared_gently++;
        })));
    }

    utils::clear_gently(v).get();
    BOOST_CHECK(v.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_unordered_map) {
    struct X {
        std::unique_ptr<std::pair<int, int>> v;
        std::function<void (int, int)> on_clear;
        X(int i, int j, std::function<void (int, int)> f) : v(std::make_unique<std::pair<int, int>>(i, j)), on_clear(std::move(f)) {}
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
        future<> clear_gently() noexcept {
            std::pair<int, int>* p = v.get();
            on_clear(p->first, p->second);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int top_count = 10;
    constexpr int count = 10;
    std::unordered_map<int, std::vector<X>> c;
    int cleared_gently = 0;

    for (int i = 0; i < top_count; i++) {
        std::vector<X> v;
        v.reserve(count);
        for (int j = 0; j < count; j++) {
            v.emplace_back(X(i, j, [&cleared_gently] (int, int) {
                cleared_gently++;
            }));
        }
        c.insert(std::pair<int, std::vector<X>>(i, std::move(v)));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_nested_container) {
    struct X {
        std::unique_ptr<std::pair<int, int>> v;
        std::function<void (int, int)> on_clear;
        X(int i, int j, std::function<void (int, int)> f) : v(std::make_unique<std::pair<int, int>>(i, j)), on_clear(std::move(f)) {}
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) {}
        future<> clear_gently() noexcept {
            std::pair<int, int>* p = v.get();
            on_clear(p->first, p->second);
            v.reset();
            return make_ready_future<>();
        }
    };
    constexpr int top_count = 10;
    constexpr int count = 10;
    std::list<std::unordered_map<int, X>> c;
    int cleared_gently = 0;

    for (int i = 0; i < top_count; i++) {
        std::unordered_map<int, X> m;
        for (int j = 0; j < count; j++) {
            m.insert(std::pair<int, X>(j, X(i, j, [&cleared_gently] (int, int) {
                cleared_gently++;
            })));
        }
        c.push_back(std::move(m));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * count);
}

SEASTAR_THREAD_TEST_CASE(test_clear_gently_multi_nesting) {
    struct X {
        std::unique_ptr<std::tuple<int, int, int>> v;
        std::function<void (int, int, int)> on_clear;
        X(int i, int j, int k, std::function<void (int, int, int)> f) : v(std::make_unique<std::tuple<int, int, int>>(i, j, k)), on_clear(std::move(f)) { }
        X(X&& x) noexcept : v(std::move(x.v)), on_clear(std::move(x.on_clear)) { }
        future<> clear_gently() {
            on_clear(std::get<0>(*v), std::get<1>(*v), std::get<2>(*v));
            v.reset();
            return make_ready_future<>();
        }
    };
    struct V {
        std::vector<X> v;
        V(int i, int j, int count, std::function<void (int, int, int)> f) {
            v.reserve(count);
            for (int k = 0; k < count; k++) {
                v.emplace_back(X(i, j, k, f));
            }
        }
        future<> clear_gently() {
            return utils::clear_gently(v);
        }
    };
    constexpr int top_count = 10;
    constexpr int mid_count = 10;
    constexpr int count = 10;
    std::vector<std::map<int, V>> c;
    int cleared_gently = 0;

    for (int i = 0; i < top_count; i++) {
        std::map<int, V> m;
        for (int j = 0; j < mid_count; j++) {
            m.insert(std::pair<int, V>(j, V(i, j, count, [&cleared_gently] (int, int, int) {
                cleared_gently++;
            })));
        }
        c.push_back(std::move(m));
    }

    utils::clear_gently(c).get();
    BOOST_CHECK(c.empty());
    BOOST_REQUIRE_EQUAL(cleared_gently, top_count * mid_count * count);
}
