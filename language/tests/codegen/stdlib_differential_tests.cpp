// Differential tests: every scalar domain of the HGL standard library against
// the native hgraph operator it re-implements.
//
// The hgraph_std ``test`` blocks check each HGL body against expectations
// written by the same author, and generated_catalogue_tests.cpp compares a
// hand-picked input per operator. This file drives both implementations over
// seeded, generated tick sequences for every instantiated or concrete scalar
// domain, including gaps, repeats and boundary values, and requires identical
// per-cycle output (NaN equal to NaN, signed zeros distinguished) or that both
// raise the same error. A coverage ratchet reads the hgraph_std implementation sources so a
// new HGL domain cannot be added without a differential here, or an explicit
// exclusion with its reason.
//
// HGL_DIFFERENTIAL_CASES (default 24) sets the generated sequences per domain.

#include <operators.h>
#include <standard.h>

#include "wiring/backend.h"

#include <hgraph/lib/std/operators/operators.h>
#include <hgraph/lib/std/std_nodes.h>
#include <hgraph/lib/testing/check_output.h>
#include <hgraph/lib/testing/eval_node.h>
#include <hgraph/util/environment.h>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

using namespace hgraph;
using namespace hgraph::testing;
namespace scalar   = hgraph_::operators_::operators;
namespace standard = hgraph_::std_::operators;

#ifndef HGL_STD_IMPL_DIR
#error "HGL_STD_IMPL_DIR must name hgraph_std/hgl/hgraph/impl"
#endif

namespace
{
    using Rng = std::mt19937_64;
    using hgraph::testing::detail::format_seq;

    void register_catalogue() {
        hgl::wiring::ensure_session();
        hgraph_::operators_::register_operators();
        hgraph_::std_::register_operators();
    }

    [[nodiscard]] std::size_t case_count() {
        if (const auto text = hgraph::environment_variable("HGL_DIFFERENTIAL_CASES")) {
            if (const auto value = std::strtoul(text->c_str(), nullptr, 10); value > 0) { return value; }
        }
        return 24;
    }

    [[nodiscard]] std::uint64_t seed_for(std::string_view domain, std::size_t index) {
        // FNV-1a, so a reported (domain, case) reproduces on every platform.
        std::uint64_t hash = 1469598103934665603ULL;
        for (const char c : domain) { hash = (hash ^ static_cast<unsigned char>(c)) * 1099511628211ULL; }
        return hash ^ (0x9E3779B97F4A7C15ULL * (index + 1));
    }

    // ---- value generators --------------------------------------------------

    /** Value range per operand: Small keeps checked kernels near their edges;
        Arith keeps sums and products inside int64 (signed overflow is
        undefined in both implementations, not a behaviour to compare); Any
        adds the full boundary set where the operation is total. */
    enum class Spread { Small, Arith, Shift, Any };

    template <typename T> T pick(Rng &rng, const std::vector<T> &choices) {
        return choices[std::uniform_int_distribution<std::size_t>(0, choices.size() - 1)(rng)];
    }

    template <typename T> T draw(Rng &rng, Spread range);

    template <> Bool draw<Bool>(Rng &rng, Spread) { return std::bernoulli_distribution(0.5)(rng); }

    template <> Int draw<Int>(Rng &rng, Spread range) {
        std::uniform_int_distribution<int> kind(0, 3);
        switch (range) {
            case Spread::Small: return std::uniform_int_distribution<Int>(-10, 10)(rng);
            case Spread::Shift: return std::uniform_int_distribution<Int>(-2, 70)(rng);
            case Spread::Arith:
                switch (kind(rng)) {
                    case 0: return pick<Int>(rng, {0, 1, -1, 2, -2, 7, -7});
                    case 1: return std::uniform_int_distribution<Int>(-1000, 1000)(rng);
                    case 2: return std::uniform_int_distribution<Int>(-(Int{1} << 31), Int{1} << 31)(rng);
                    default: return std::uniform_int_distribution<Int>(-100, 100)(rng);
                }
            case Spread::Any:
                switch (kind(rng)) {
                    case 0:
                        return pick<Int>(rng, {0, 1, -1, std::numeric_limits<Int>::max(), std::numeric_limits<Int>::min(),
                                               Int{1} << 53, -(Int{1} << 53)});
                    case 1: return std::uniform_int_distribution<Int>(-1000, 1000)(rng);
                    default: return std::uniform_int_distribution<Int>(std::numeric_limits<Int>::min(),
                                                                      std::numeric_limits<Int>::max())(rng);
                }
        }
        return 0;
    }

    template <> Float draw<Float>(Rng &rng, Spread range) {
        constexpr Float inf = std::numeric_limits<Float>::infinity();
        std::uniform_int_distribution<int> kind(0, 4);
        if (range == Spread::Small) { return std::uniform_int_distribution<int>(-20, 20)(rng) / 4.0; }
        switch (kind(rng)) {
            case 0:
                return pick<Float>(rng, {0.0, -0.0, 1.0, -1.0, 0.5, -0.5, 2.5, -2.5, 1e-300, -1e-300, 1e300, -1e300,
                                         inf, -inf, std::numeric_limits<Float>::quiet_NaN(),
                                         std::numeric_limits<Float>::denorm_min()});
            case 1: return std::uniform_int_distribution<int>(-40, 40)(rng) / 8.0;
            case 2: return std::uniform_real_distribution<Float>(-1e6, 1e6)(rng);
            case 3: return std::uniform_real_distribution<Float>(-1.0, 1.0)(rng);
            default: return static_cast<Float>(std::uniform_int_distribution<int>(-100, 100)(rng));
        }
    }

    template <> Str draw<Str>(Rng &rng, Spread) {
        return pick<Str>(rng, {"", "a", "b", "ab", "ba", "abc", "abcabc", "A", "z", "hgraph", "\xc3\xa9", Str{"a\0b", 3}});
    }

    template <> Date draw<Date>(Rng &rng, Spread) {
        std::uniform_int_distribution<int> kind(0, 2);
        const int days = kind(rng) == 0
                             ? pick<int>(rng, {0, -1, 59, 60, 11016, 19782, 19783, -25567, 47482})  // epoch, leap days, 1900, 2100
                             : std::uniform_int_distribution<int>(-25567, 47482)(rng);
        return Date{std::chrono::sys_days{std::chrono::days{days}}};
    }

    template <> TimeDelta draw<TimeDelta>(Rng &rng, Spread) {
        std::uniform_int_distribution<int> kind(0, 2);
        constexpr std::int64_t day = 86'400'000'000LL;
        switch (kind(rng)) {
            case 0: return TimeDelta{pick<std::int64_t>(rng, {0, 1, -1, day, -day, day - 1, -day + 1, 3'600'000'001})};
            case 1: return TimeDelta{std::uniform_int_distribution<std::int64_t>(-10 * day, 10 * day)(rng)};
            default: return TimeDelta{std::uniform_int_distribution<std::int64_t>(-1'000'000, 1'000'000)(rng)};
        }
    }

    template <> DateTime draw<DateTime>(Rng &rng, Spread range) {
        std::uniform_int_distribution<int> kind(0, 2);
        constexpr std::int64_t day = 86'400'000'000LL;
        if (kind(rng) == 0) { return DateTime{TimeDelta{pick<std::int64_t>(rng, {0, -1, 1, day, -day, 951'782'400'000'000})}}; }
        return DateTime{TimeDelta{std::uniform_int_distribution<std::int64_t>(-70 * 365 * day, 130 * 365 * day)(rng)}} +
               (kind(rng) == 1 ? draw<TimeDelta>(rng, range) : TimeDelta{0});
    }

    template <> Time draw<Time>(Rng &rng, Spread) {
        constexpr std::int64_t day = 86'400'000'000LL;
        std::uniform_int_distribution<int> kind(0, 1);
        return Time{kind(rng) == 0 ? pick<std::int64_t>(rng, {0, 1, day - 1, 3'661'123'456, 43'200'000'000})
                                   : std::uniform_int_distribution<std::int64_t>(0, day - 1)(rng)};
    }

    /** A sparse tick sequence: gaps, repeats of the previous value (dedup and
        no-change policies) and, for the first tick, possibly a gap. */
    template <typename T> std::vector<std::optional<T>> sequence(Rng &rng, Spread range, std::size_t length = 20) {
        std::vector<std::optional<T>> ticks;
        ticks.reserve(length);
        std::bernoulli_distribution gap(0.25);
        std::bernoulli_distribution repeat(0.2);
        std::optional<T> previous;
        for (std::size_t i = 0; i < length; ++i) {
            if (gap(rng)) {
                ticks.emplace_back(std::nullopt);
                continue;
            }
            T value = (previous.has_value() && repeat(rng)) ? *previous : draw<T>(rng, range);
            previous = value;
            ticks.emplace_back(value);
        }
        return ticks;
    }

    // ---- outcome comparison -------------------------------------------------

    struct Outcome
    {
        bool threw{false};
        std::string message{};
        std::vector<std::optional<Value>> ticks{};
    };

    template <typename Op, typename... Explicit, typename... Args> Outcome run(const Args &...args) {
        try {
            auto result = eval_node<Op, Explicit...>(args...);
            Outcome outcome;
            outcome.ticks.reserve(result.size());
            for (auto &tick : result) {
                if (!tick.has_value()) { outcome.ticks.emplace_back(std::nullopt); }
                else if constexpr (std::is_same_v<std::decay_t<decltype(*tick)>, Value>) { outcome.ticks.emplace_back(*tick); }
                else { outcome.ticks.emplace_back(Value{*tick}); }
            }
            return outcome;
        } catch (const std::exception &error) { return Outcome{true, error.what(), {}}; }
    }

    [[nodiscard]] bool same_value(const Value &lhs, const Value &rhs) {
        const auto *a = lhs.view().try_as<Float>();
        const auto *b = rhs.view().try_as<Float>();
        if (a != nullptr || b != nullptr) {
            if (a == nullptr || b == nullptr) { return false; }
            if (std::isnan(*a) || std::isnan(*b)) { return std::isnan(*a) && std::isnan(*b); }
            return *a == *b && std::signbit(*a) == std::signbit(*b);
        }
        if (lhs.equals(rhs)) { return true; }
        // A NaN nested in a collection or bundle defeats Value::equals on both
        // sides alike; compare the rendering when a NaN is present.
        const std::string a_text = lhs.to_string();
        const std::string b_text = rhs.to_string();
        return a_text == b_text && a_text.find("nan") != std::string::npos;
    }

    [[nodiscard]] bool same_ticks(const Outcome &lhs, const Outcome &rhs) {
        if (lhs.ticks.size() != rhs.ticks.size()) { return false; }
        for (std::size_t i = 0; i < lhs.ticks.size(); ++i) {
            const auto &a = lhs.ticks[i];
            const auto &b = rhs.ticks[i];
            if (a.has_value() != b.has_value()) { return false; }
            if (a.has_value() && !same_value(*a, *b)) { return false; }
        }
        return true;
    }

    /** The error a node raised, without the node-identity prefix and the
        activation back-trace: both name the implementation node, which
        legitimately differs between an HGL body and the native node. */
    [[nodiscard]] std::string error_text(std::string message) {
        static const std::regex prefix{R"(^node\[\d+ '[^']*'\] evaluate failed: )"};
        message = std::regex_replace(message, prefix, "");
        if (const auto trace = message.find(" Activation Back Trace"); trace != std::string::npos) { message.resize(trace); }
        if (const auto trace = message.find("\nActivation Back Trace"); trace != std::string::npos) { message.resize(trace); }
        return message;
    }

    [[nodiscard]] std::string render(const Outcome &outcome) {
        return outcome.threw ? "raised: " + outcome.message : format_seq(outcome.ticks);
    }

    template <typename T> std::string render_input(const std::vector<std::optional<T>> &ticks) { return format_seq(ticks); }
    template <typename T> std::string render_input(const T &value) { return Value{value}.to_string(); }

    /** Accumulates mismatches across a test case so one run reports all of them. */
    struct Report
    {
        std::size_t compared{0};
        std::size_t both_raised{0};
        std::vector<std::string> mismatches{};

        void check() const {
            INFO("compared " << compared << " generated cases; both raised in " << both_raised);
            std::ostringstream text;
            for (const auto &mismatch : mismatches) { text << mismatch << "\n"; }
            INFO(text.str());
            CHECK(mismatches.empty());
        }
    };

    /** Which generated case a comparison runs: its index and the key its inputs
        were seeded from. Several domains often share one draw (a stream's five
        operators, a source's const and default), so the key can differ from the
        domain reported; an empty key means the domain itself. */
    struct Generated
    {
        std::size_t index{};
        std::string seed_key{};

        Generated(std::size_t case_index) : index{case_index} {}  // NOLINT(google-explicit-constructor)
        Generated(std::string key, std::size_t case_index) : index{case_index}, seed_key{std::move(key)} {}
    };

    /** The mismatch heading: the domain, the case, and the seed that drew its inputs. */
    [[nodiscard]] std::string case_heading(std::string_view domain, const Generated &generated) {
        const std::string key = generated.seed_key.empty() ? std::string{domain} : generated.seed_key;
        std::ostringstream text;
        text << domain << " case " << generated.index << " (seed " << seed_for(key, generated.index) << " from '" << key << "')";
        return text.str();
    }

    template <typename Hgl, typename Native, typename... Args>
    void compare(Report &report, std::string_view domain, const Generated &generated, const Args &...args) {
        const Outcome hgl    = run<Hgl>(args...);
        const Outcome native = run<Native>(args...);
        ++report.compared;
        if (hgl.threw && native.threw && error_text(hgl.message) == error_text(native.message)) {
            ++report.both_raised;
            return;
        }
        if (!hgl.threw && !native.threw && same_ticks(hgl, native)) { return; }
        std::ostringstream text;
        text << case_heading(domain, generated);
        ((text << "\n  input:  " << render_input(args)), ...);
        text << "\n  hgl:    " << render(hgl) << "\n  native: " << render(native);
        report.mismatches.push_back(text.str());
    }

    template <typename... Schemas> struct schemas
    {
    };

    /** As ``compare``, with explicit input schemas for inputs whose harness
        element is a type-erased ``Value`` (sets and maps). */
    template <typename Hgl, typename Native, typename... Schemas, typename... Args>
    void compare_typed(Report &report, std::string_view domain, const Generated &generated, schemas<Schemas...>,
                       const Args &...args) {
        const Outcome hgl    = run<Hgl, Schemas...>(args...);
        const Outcome native = run<Native, Schemas...>(args...);
        ++report.compared;
        if (hgl.threw && native.threw && error_text(hgl.message) == error_text(native.message)) {
            ++report.both_raised;
            return;
        }
        if (!hgl.threw && !native.threw && same_ticks(hgl, native)) { return; }
        std::ostringstream text;
        text << case_heading(domain, generated);
        ((text << "\n  input:  " << render_input(args)), ...);
        text << "\n  hgl:    " << render(hgl) << "\n  native: " << render(native);
        report.mismatches.push_back(text.str());
    }

    /** ``count`` distinct values of ``T`` (by ``==``), drawn without NaN so set
        and key identity is well defined on both sides. */
    template <typename T> std::vector<T> universe(Rng &rng, std::size_t count) {
        std::vector<T> values;
        for (std::size_t attempt = 0; values.size() < count && attempt < 64; ++attempt) {
            T value = draw<T>(rng, Spread::Small);
            if (std::find(values.begin(), values.end(), value) == values.end()) { values.push_back(value); }
        }
        return values;
    }

    /** Set deltas over a small universe: each tick adds members not present and
        removes members that are, so both sides see real membership changes. */
    template <typename T>
    std::vector<std::optional<Value>> set_sequence(Rng &rng, const std::vector<T> &members, std::size_t length = 16) {
        std::vector<std::optional<Value>> ticks;
        std::vector<T> current;
        std::bernoulli_distribution gap(0.25);
        std::bernoulli_distribution take(0.4);
        for (std::size_t i = 0; i < length; ++i) {
            if (gap(rng)) {
                ticks.emplace_back(std::nullopt);
                continue;
            }
            std::vector<T> added;
            std::vector<T> removed;
            for (const T &member : members) {
                const bool present = std::find(current.begin(), current.end(), member) != current.end();
                if (!take(rng)) { continue; }
                (present ? removed : added).push_back(member);
            }
            for (const T &member : removed) { std::erase(current, member); }
            for (const T &member : added) { current.push_back(member); }
            ticks.emplace_back(set_delta<T>(added, removed));
        }
        return ticks;
    }

    template <typename T>
    std::vector<std::optional<T>> drawn_from(Rng &rng, const std::vector<T> &members, std::size_t length = 16) {
        std::vector<std::optional<T>> ticks;
        std::bernoulli_distribution gap(0.25);
        std::uniform_int_distribution<std::size_t> index(0, members.empty() ? 0 : members.size() - 1);
        for (std::size_t i = 0; i < length; ++i) {
            if (gap(rng) || members.empty()) { ticks.emplace_back(std::nullopt); }
            else { ticks.emplace_back(members[index(rng)]); }
        }
        return ticks;
    }

    // ---- domain drivers ----------------------------------------------------

    template <typename Hgl, typename Native, typename A>
    void unary(Report &report, std::string_view domain, Spread range = Spread::Any) {
        for (std::size_t i = 0; i < case_count(); ++i) {
            Rng rng{seed_for(domain, i)};
            compare<Hgl, Native>(report, domain, i, sequence<A>(rng, range));
        }
    }

    template <typename Hgl, typename Native, typename A, typename B>
    void binary(Report &report, std::string_view domain, Spread lhs = Spread::Arith, Spread rhs = Spread::Arith) {
        for (std::size_t i = 0; i < case_count(); ++i) {
            Rng rng{seed_for(domain, i)};
            const auto a = sequence<A>(rng, lhs);
            const auto b = sequence<B>(rng, rhs);
            compare<Hgl, Native>(report, domain, i, a, b);
        }
    }

    /** All eight scalar types, for stream operators instantiated over each. */
    template <template <typename> class Body> void each_scalar(Report &report) {
        Body<Bool>{}(report, "bool");
        Body<Int>{}(report, "i64");
        Body<Float>{}(report, "f64");
        Body<Str>{}(report, "str");
        Body<Date>{}(report, "date");
        Body<Time>{}(report, "time");
        Body<DateTime>{}(report, "datetime");
        Body<TimeDelta>{}(report, "duration");
    }

    std::string domain_name(std::string_view op, std::string_view types) {
        return std::string{op} + "<" + std::string{types} + ">";
    }

    template <typename From, typename To> struct NativeConvert
    {
        static Port<TS<To>> compose(Wiring &w, Port<TS<From>> ts) { return wire<stdlib::convert, TS<To>>(w, ts); }
    };

    /** The domains this file drives, as hgraph_std spells them, for the ratchet. */
    std::set<std::string> &covered() {
        static std::set<std::string> domains;
        return domains;
    }

    void cover(std::string domain) { covered().insert(std::move(domain)); }
}  // namespace

// ---- arithmetic -----------------------------------------------------------------

#define HGL_BINARY(OP, NATIVE, A, B, NAME, LHS_RANGE, RHS_RANGE)                                                       \
    do {                                                                                                               \
        cover(NAME);                                                                                                   \
        binary<scalar::OP, stdlib::NATIVE, A, B>(report, NAME, LHS_RANGE, RHS_RANGE);                                 \
    } while (false)

#define HGL_UNARY(NS, OP, NATIVE, A, NAME, RANGE)                                                                      \
    do {                                                                                                               \
        cover(NAME);                                                                                                   \
        unary<NS::OP, stdlib::NATIVE, A>(report, NAME, RANGE);                                                         \
    } while (false)

TEST_CASE("differential: numeric arithmetic matches the native operators", "[codegen][differential]") {
    register_catalogue();
    Report report;
#define HGL_NUMERIC_FAMILY(OP, I_I)                                                                                    \
    HGL_BINARY(OP, OP, Int, Int, domain_name(#OP, "i64,i64," I_I), Spread::Arith, Spread::Arith);                       \
    HGL_BINARY(OP, OP, Int, Float, domain_name(#OP, "i64,f64,f64"), Spread::Arith, Spread::Any);                         \
    HGL_BINARY(OP, OP, Float, Int, domain_name(#OP, "f64,i64,f64"), Spread::Any, Spread::Arith);                         \
    HGL_BINARY(OP, OP, Float, Float, domain_name(#OP, "f64,f64,f64"), Spread::Any, Spread::Any)
    HGL_NUMERIC_FAMILY(add_, "i64");
    HGL_NUMERIC_FAMILY(sub_, "i64");
    HGL_NUMERIC_FAMILY(mul_, "i64");
    HGL_NUMERIC_FAMILY(div_, "f64");
    HGL_NUMERIC_FAMILY(floordiv_, "i64");
    HGL_NUMERIC_FAMILY(mod_, "i64");
#undef HGL_NUMERIC_FAMILY
    HGL_BINARY(add_, add_, Str, Str, "add_<str,str,str>", Spread::Any, Spread::Any);
    HGL_BINARY(pow_, pow_, Int, Int, "pow_(i64,i64)", Spread::Small, Spread::Shift);
    HGL_BINARY(pow_, pow_, Float, Float, "pow_(f64,f64)", Spread::Any, Spread::Small);
    HGL_BINARY(pow_, pow_, Int, Float, "pow_(i64,f64)", Spread::Small, Spread::Small);
    HGL_BINARY(pow_, pow_, Float, Int, "pow_(f64,i64)", Spread::Any, Spread::Small);
    HGL_BINARY(lshift_, lshift_, Int, Int, "lshift_(i64,i64)", Spread::Any, Spread::Shift);
    HGL_BINARY(rshift_, rshift_, Int, Int, "rshift_(i64,i64)", Spread::Any, Spread::Shift);
    HGL_BINARY(mean, mean, Int, Int, "mean(i64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(mean, mean, Float, Float, "mean(f64,f64)", Spread::Any, Spread::Any);
    HGL_BINARY(mean, mean, Int, Float, "mean(i64,f64)", Spread::Any, Spread::Any);
    HGL_BINARY(mean, mean, Float, Int, "mean(f64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, Int, Int, "min_(i64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, Float, Float, "min_(f64,f64)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, Str, Str, "min_(str,str)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, Date, Date, "min_(date,date)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, DateTime, DateTime, "min_(datetime,datetime)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, TimeDelta, TimeDelta, "min_(duration,duration)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, Int, Float, "min_(i64,f64)", Spread::Any, Spread::Any);
    HGL_BINARY(min_, min_, Float, Int, "min_(f64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, Int, Int, "max_(i64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, Float, Float, "max_(f64,f64)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, Str, Str, "max_(str,str)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, Date, Date, "max_(date,date)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, DateTime, DateTime, "max_(datetime,datetime)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, TimeDelta, TimeDelta, "max_(duration,duration)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, Int, Float, "max_(i64,f64)", Spread::Any, Spread::Any);
    HGL_BINARY(max_, max_, Float, Int, "max_(f64,i64)", Spread::Any, Spread::Any);
    HGL_UNARY(scalar, neg_, neg_, Int, "neg_<i64,i64>", Spread::Arith);
    HGL_UNARY(scalar, neg_, neg_, Float, "neg_<f64,f64>", Spread::Any);
    HGL_UNARY(scalar, abs_, abs_, Int, "abs_(i64)", Spread::Arith);
    HGL_UNARY(scalar, abs_, abs_, Float, "abs_(f64)", Spread::Any);
    HGL_UNARY(scalar, pos_, pos_, Int, "pos_(i64)", Spread::Any);
    HGL_UNARY(scalar, pos_, pos_, Float, "pos_(f64)", Spread::Any);
    HGL_UNARY(scalar, pos_, pos_, TimeDelta, "pos_(duration)", Spread::Any);
    HGL_UNARY(scalar, sign, sign, Int, "sign(i64)", Spread::Any);
    HGL_UNARY(scalar, sign, sign, Float, "sign(f64)", Spread::Any);
    HGL_UNARY(scalar, ln, ln, Float, "ln(f64)", Spread::Any);
    HGL_BINARY(round_, round_, Float, Int, "round_(f64,i64)", Spread::Any, Spread::Small);
    report.check();
}

// ---- comparison, Boolean and bit operators ----------------------------------------

TEST_CASE("differential: comparison, Boolean and bit operators match the native operators", "[codegen][differential]") {
    register_catalogue();
    Report report;
#define HGL_ORDER_FAMILY(OP)                                                                                           \
    HGL_BINARY(OP, OP, Int, Int, domain_name(#OP, "i64,i64"), Spread::Small, Spread::Small);                            \
    HGL_BINARY(OP, OP, Float, Float, domain_name(#OP, "f64,f64"), Spread::Any, Spread::Any);                            \
    HGL_BINARY(OP, OP, Int, Float, domain_name(#OP, "i64,f64"), Spread::Small, Spread::Any);                            \
    HGL_BINARY(OP, OP, Float, Int, domain_name(#OP, "f64,i64"), Spread::Any, Spread::Small);                            \
    HGL_BINARY(OP, OP, Str, Str, domain_name(#OP, "str,str"), Spread::Any, Spread::Any);                                \
    HGL_BINARY(OP, OP, Date, Date, domain_name(#OP, "date,date"), Spread::Any, Spread::Any);                            \
    HGL_BINARY(OP, OP, DateTime, DateTime, domain_name(#OP, "datetime,datetime"), Spread::Any, Spread::Any);            \
    HGL_BINARY(OP, OP, TimeDelta, TimeDelta, domain_name(#OP, "duration,duration"), Spread::Any, Spread::Any)
    HGL_ORDER_FAMILY(lt_);
    HGL_ORDER_FAMILY(le_);
    HGL_ORDER_FAMILY(gt_);
    HGL_ORDER_FAMILY(ge_);
    HGL_ORDER_FAMILY(ne_);
#undef HGL_ORDER_FAMILY
    HGL_BINARY(ne_, ne_, Bool, Bool, "ne_<bool,bool>", Spread::Any, Spread::Any);
    HGL_BINARY(ne_, ne_, Time, Time, "ne_<time,time>", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, Int, Int, "eq_<i64,i64>", Spread::Small, Spread::Small);
    HGL_BINARY(eq_, eq_, Str, Str, "eq_<str,str>", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, Bool, Bool, "eq_<bool,bool>", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, Date, Date, "eq_<date,date>", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, DateTime, DateTime, "eq_<datetime,datetime>", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, TimeDelta, TimeDelta, "eq_<duration,duration>", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, Time, Time, "eq_<time,time>", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, Float, Float, "eq_(f64,f64)", Spread::Any, Spread::Any);
    HGL_BINARY(eq_, eq_, Int, Float, "eq_(i64,f64)", Spread::Small, Spread::Any);
    HGL_BINARY(eq_, eq_, Float, Int, "eq_(f64,i64)", Spread::Any, Spread::Small);
#define HGL_TRUTH_FAMILY(OP)                                                                                           \
    HGL_BINARY(OP, OP, Bool, Bool, #OP "(bool,bool)", Spread::Any, Spread::Any);                                         \
    HGL_BINARY(OP, OP, Int, Int, #OP "(i64,i64)", Spread::Small, Spread::Small);                                         \
    HGL_BINARY(OP, OP, Float, Float, #OP "(f64,f64)", Spread::Any, Spread::Any);                                         \
    HGL_BINARY(OP, OP, Str, Str, #OP "(str,str)", Spread::Any, Spread::Any);                                             \
    HGL_BINARY(OP, OP, Int, Float, #OP "(i64,f64)", Spread::Small, Spread::Any);                                         \
    HGL_BINARY(OP, OP, Float, Int, #OP "(f64,i64)", Spread::Any, Spread::Small)
    HGL_TRUTH_FAMILY(and_);
    HGL_TRUTH_FAMILY(or_);
#undef HGL_TRUTH_FAMILY
    HGL_UNARY(scalar, not_, not_, Bool, "not_(bool)", Spread::Any);
    HGL_UNARY(scalar, not_, not_, Int, "not_(i64)", Spread::Small);
    HGL_UNARY(scalar, not_, not_, Float, "not_(f64)", Spread::Any);
    HGL_UNARY(scalar, not_, not_, Str, "not_(str)", Spread::Any);
    HGL_BINARY(bit_and, bit_and, Int, Int, "bit_and(i64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(bit_and, bit_and, Bool, Bool, "bit_and(bool,bool)", Spread::Any, Spread::Any);
    HGL_BINARY(bit_or, bit_or, Int, Int, "bit_or(i64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(bit_or, bit_or, Bool, Bool, "bit_or(bool,bool)", Spread::Any, Spread::Any);
    HGL_BINARY(bit_xor, bit_xor, Int, Int, "bit_xor(i64,i64)", Spread::Any, Spread::Any);
    HGL_BINARY(bit_xor, bit_xor, Bool, Bool, "bit_xor(bool,bool)", Spread::Any, Spread::Any);
    HGL_UNARY(scalar, invert_, invert_, Int, "invert_(i64)", Spread::Any);
    HGL_UNARY(scalar, invert_, invert_, Bool, "invert_(bool)", Spread::Any);
    HGL_BINARY(contains_, contains_, Str, Str, "contains_(str,str)", Spread::Any, Spread::Any);
    report.check();
}

#undef HGL_BINARY

// ---- scalar streams --------------------------------------------------------------

namespace
{
    template <typename T> struct StreamBodies
    {
        void operator()(Report &report, std::string_view type) const {
            const auto name = [&](std::string_view op) { return domain_name(op, type); };
            for (const std::string_view op : {"sample", "drop", "filter_", "take", "freeze"}) { cover(name(op)); }
            for (std::size_t i = 0; i < case_count(); ++i) {
                const Generated generated{name("stream"), i};
                Rng             rng{seed_for(generated.seed_key, i)};
                const auto ticks     = sequence<T>(rng, Spread::Any);
                const auto condition = sequence<Bool>(rng, Spread::Any);
                const Int count      = std::uniform_int_distribution<Int>(-1, 8)(rng);
                compare<standard::sample, stdlib::sample>(report, name("sample"), generated, condition, ticks);
                compare<standard::drop, stdlib::drop>(report, name("drop"), generated, ticks, count);
                compare<standard::filter_, stdlib::filter_>(report, name("filter_"), generated, condition, ticks);
                compare<standard::take, stdlib::take>(report, name("take"), generated, ticks, count);
                compare<standard::freeze, stdlib::freeze>(report, name("freeze"), generated, condition, ticks);
            }
        }
    };

    template <typename T> struct DedupBody
    {
        void operator()(Report &report, std::string_view type) const {
            const auto domain = "dedup(" + std::string{type} + ")";
            cover(domain);
            unary<standard::dedup, stdlib::dedup, T>(report, domain);
        }
    };
}  // namespace

TEST_CASE("differential: a mismatch names the seed that drew its inputs", "[codegen][differential]") {
    // The five stream operators share one draw seeded from the stream key.
    const std::string shared = case_heading("sample(i64)", Generated{"stream(i64)", 3});
    CHECK(shared.find("seed " + std::to_string(seed_for("stream(i64)", 3)) + " from 'stream(i64)'") != std::string::npos);
    // A domain that seeds its own draw reports itself.
    const std::string own = case_heading("dedup(i64)", Generated{7});
    CHECK(own.find("seed " + std::to_string(seed_for("dedup(i64)", 7)) + " from 'dedup(i64)'") != std::string::npos);
}

TEST_CASE("differential: scalar stream operators match the native operators", "[codegen][differential]") {
    register_catalogue();
    Report report;
    each_scalar<StreamBodies>(report);
    each_scalar<DedupBody>(report);
    cover("dedup_float(f64,f64)");
    for (std::size_t i = 0; i < case_count(); ++i) {
        const Generated generated{"dedup_float", i};
        Rng             rng{seed_for(generated.seed_key, i)};
        const auto ticks    = sequence<Float>(rng, Spread::Small);
        const Float abs_tol = pick<Float>(rng, {0.0, 0.25, 0.5, 1.0});
        compare<standard::dedup_float, stdlib::dedup>(report, "dedup_float(f64,f64)", generated, ticks, abs_tol);
    }
    cover("until_true(bool)");
    unary<standard::until_true, stdlib::until_true, Bool>(report, "until_true(bool)");
    cover("if_true(bool,bool)");
    for (std::size_t i = 0; i < case_count(); ++i) {
        const Generated generated{"if_true", i};
        Rng             rng{seed_for(generated.seed_key, i)};
        const auto ticks = sequence<Bool>(rng, Spread::Any);
        compare<standard::if_true, stdlib::if_true>(report, "if_true(bool,bool)", generated, ticks, Bool{false});
        compare<standard::if_true, stdlib::if_true>(report, "if_true(bool,bool)", generated, ticks, Bool{true});
    }
    report.check();
}

TEST_CASE("differential: folds and running extrema match the native operators", "[codegen][differential]") {
    register_catalogue();
    Report report;
    cover("sum(i64)");
    unary<standard::sum, stdlib::sum_, Int>(report, "sum(i64)", Spread::Arith);
    cover("sum(f64)");
    unary<standard::sum, stdlib::sum_, Float>(report, "sum(f64)", Spread::Any);
    cover("mean(i64)");
    unary<standard::mean, stdlib::mean, Int>(report, "mean(i64)", Spread::Arith);
    cover("mean(f64)");
    unary<standard::mean, stdlib::mean, Float>(report, "mean(f64)", Spread::Any);
    cover("sum_reset(i64,bool)");
    cover("sum_reset(f64,bool)");
    for (std::size_t i = 0; i < case_count(); ++i) {
        const Generated generated{"sum_reset", i};
        Rng             rng{seed_for(generated.seed_key, i)};
        const auto integers = sequence<Int>(rng, Spread::Arith);
        const auto floating = sequence<Float>(rng, Spread::Any);
        const auto reset    = sequence<Bool>(rng, Spread::Any);
        compare<standard::sum_reset, stdlib::sum_>(report, "sum_reset(i64,bool)", generated, integers, reset);
        compare<standard::sum_reset, stdlib::sum_>(report, "sum_reset(f64,bool)", generated, floating, reset);
    }
#define HGL_RUNNING(OP, A, TYPE)                                                                                       \
    do {                                                                                                               \
        cover(#OP "(" TYPE ")");                                                                                       \
        unary<standard::OP, stdlib::OP, A>(report, #OP "(" TYPE ")");                                                  \
    } while (false)
    HGL_RUNNING(min_, Int, "i64");
    HGL_RUNNING(min_, Float, "f64");
    HGL_RUNNING(min_, Str, "str");
    HGL_RUNNING(min_, Date, "date");
    HGL_RUNNING(min_, DateTime, "datetime");
    HGL_RUNNING(min_, TimeDelta, "duration");
    HGL_RUNNING(min_, Bool, "bool");
    HGL_RUNNING(max_, Int, "i64");
    HGL_RUNNING(max_, Float, "f64");
    HGL_RUNNING(max_, Str, "str");
    HGL_RUNNING(max_, Date, "date");
    HGL_RUNNING(max_, DateTime, "datetime");
    HGL_RUNNING(max_, TimeDelta, "duration");
    HGL_RUNNING(max_, Bool, "bool");
#undef HGL_RUNNING
    report.check();
}

// ---- temporal components and conversions -----------------------------------------

TEST_CASE("differential: temporal components and conversions match the native operators", "[codegen][differential]") {
    register_catalogue();
    Report report;
#define HGL_COMPONENT(OP, A, TYPE)                                                                                     \
    do {                                                                                                               \
        cover(#OP "(" TYPE ")");                                                                                       \
        unary<standard::OP, stdlib::OP, A>(report, #OP "(" TYPE ")");                                                  \
    } while (false)
#define HGL_DATE_COMPONENT(OP)                                                                                         \
    HGL_COMPONENT(OP, Date, "date");                                                                                   \
    HGL_COMPONENT(OP, DateTime, "datetime")
    HGL_DATE_COMPONENT(year);
    HGL_DATE_COMPONENT(month);
    HGL_DATE_COMPONENT(day);
    HGL_DATE_COMPONENT(weekday);
    HGL_DATE_COMPONENT(isoweekday);
    HGL_DATE_COMPONENT(month_of_year);
    HGL_DATE_COMPONENT(day_of_month);
#undef HGL_DATE_COMPONENT
#define HGL_TIME_COMPONENT(OP)                                                                                         \
    HGL_COMPONENT(OP, DateTime, "datetime");                                                                           \
    HGL_COMPONENT(OP, Time, "time")
    HGL_TIME_COMPONENT(hour);
    HGL_TIME_COMPONENT(minute);
    HGL_TIME_COMPONENT(second);
    HGL_TIME_COMPONENT(microsecond);
#undef HGL_TIME_COMPONENT
    HGL_COMPONENT(days, TimeDelta, "duration");
    HGL_COMPONENT(seconds, TimeDelta, "duration");
    HGL_COMPONENT(microseconds, TimeDelta, "duration");
    HGL_COMPONENT(total_seconds, TimeDelta, "duration");
    HGL_COMPONENT(timestamp, DateTime, "datetime");
    HGL_COMPONENT(datepart, DateTime, "datetime");
#undef HGL_COMPONENT
#define HGL_CONVERT(OP, FROM, TO, NAME, RANGE)                                                                         \
    do {                                                                                                               \
        cover(NAME);                                                                                                   \
        unary<standard::OP, NativeConvert<FROM, TO>, FROM>(report, NAME, RANGE);                                       \
    } while (false)
    HGL_CONVERT(to_int, Bool, Int, "to_int(bool)", Spread::Any);
    HGL_CONVERT(to_int, Float, Int, "to_int(f64)", Spread::Any);
    HGL_CONVERT(to_float, Bool, Float, "to_float(bool)", Spread::Any);
    HGL_CONVERT(to_float, Int, Float, "to_float(i64)", Spread::Any);
    HGL_CONVERT(to_bool, Int, Bool, "to_bool(i64)", Spread::Any);
    HGL_CONVERT(to_bool, Float, Bool, "to_bool(f64)", Spread::Any);
    HGL_CONVERT(to_date, DateTime, Date, "to_date(datetime)", Spread::Any);
    HGL_CONVERT(to_datetime, Date, DateTime, "to_datetime(date)", Spread::Any);
#undef HGL_CONVERT
    report.check();
}

#undef HGL_UNARY

// ---- sources, defaults, membership and keyed collectors ----------------------

namespace
{
    template <typename T> struct ScalarSources
    {
        void operator()(Report &report, std::string_view type) const {
            cover(domain_name("const", type));
            cover(domain_name("default", type));
            for (std::size_t i = 0; i < case_count(); ++i) {
                const Generated generated{domain_name("sources", type), i};
                Rng             rng{seed_for(generated.seed_key, i)};
                const T value = draw<T>(rng, Spread::Any);
                // The native const takes its delay after a type argument, so only
                // the value form compares positionally; the HGL delay is checked in
                // hgraph_std tests/control.hgl.
                compare<standard::const_, stdlib::const_>(report, domain_name("const", type), generated, value);
                const auto ts       = sequence<T>(rng, Spread::Any);
                const auto fallback = sequence<T>(rng, Spread::Any);
                compare<standard::default_, stdlib::default_>(report, domain_name("default", type), generated, ts, fallback);
            }
        }
    };

    template <typename T> struct Membership
    {
        void operator()(Report &report, std::string_view type) const {
            const std::string t{type};
            const auto item_domain   = "contains_(set<" + t + ">," + t + ")";
            const auto subset_domain = "contains_(set<" + t + ">,set<" + t + ">)";
            cover(item_domain);
            cover(subset_domain);
            for (std::size_t i = 0; i < case_count(); ++i) {
                const Generated generated{item_domain, i};
                Rng             rng{seed_for(generated.seed_key, i)};
                const auto members = universe<T>(rng, 4);
                const auto sets    = set_sequence<T>(rng, members);
                const auto subsets = set_sequence<T>(rng, members);
                const auto items   = drawn_from<T>(rng, members);
                compare_typed<standard::contains_, stdlib::contains_>(report, item_domain, generated, schemas<TSS<T>>{}, sets, items);
                compare_typed<standard::contains_, stdlib::contains_>(report, subset_domain, generated, schemas<TSS<T>, TSS<T>>{},
                                                                      sets, subsets);
            }
        }
    };

    template <typename K, typename V> struct NativeCollect
    {
        static Port<TSD<K, TS<V>>> compose(Wiring &w, Port<TS<K>> key, Port<TS<V>> ts) {
            return wire<stdlib::collect, TSD<K, TS<V>>>(w, key, ts);
        }
    };

    /** The keyed collectors are generic over the key (``collect_map<K>``) with
        one body per value type; drive every key with every value type. */
    template <typename K> struct KeyedCollectors
    {
        template <typename V> void values_of(Report &report, std::string_view key_type, std::string_view value_type) const {
            const std::string domain = std::string{key_type} + "->" + std::string{value_type};
            for (std::size_t i = 0; i < std::max<std::size_t>(case_count() / 4, 2); ++i) {
                const Generated generated{"keyed " + domain, i};
                Rng             rng{seed_for(generated.seed_key, i)};
                const auto keys   = drawn_from<K>(rng, universe<K>(rng, 3));
                const auto values = sequence<V>(rng, Spread::Any, keys.size());
                const auto remove = sequence<Bool>(rng, Spread::Any, keys.size());
                compare<standard::collect_map, NativeCollect<K, V>>(report, "collect_map " + domain, generated, keys, values);
                compare<standard::make_tsd, stdlib::make_tsd>(report, "make_tsd " + domain, generated, keys, values);
                compare<standard::make_tsd_remove, stdlib::make_tsd>(report, "make_tsd_remove " + domain, generated, keys, values,
                                                                     remove);
            }
        }

        void operator()(Report &report, std::string_view key_type) const {
            for (const std::string_view op : {"collect_map", "make_tsd", "make_tsd_remove"}) { cover(domain_name(op, key_type)); }
            values_of<Bool>(report, key_type, "bool");
            values_of<Int>(report, key_type, "i64");
            values_of<Float>(report, key_type, "f64");
            values_of<Str>(report, key_type, "str");
            values_of<Date>(report, key_type, "date");
            values_of<Time>(report, key_type, "time");
            values_of<DateTime>(report, key_type, "datetime");
            values_of<TimeDelta>(report, key_type, "duration");
        }
    };
}  // namespace

TEST_CASE("differential: sources, defaults and membership match the native operators", "[codegen][differential]") {
    register_catalogue();
    Report report;
    each_scalar<ScalarSources>(report);
    each_scalar<Membership>(report);
    report.check();
}

TEST_CASE("differential: keyed collectors match the native operators for every key and value type",
          "[codegen][differential]") {
    register_catalogue();
    Report report;
    each_scalar<KeyedCollectors>(report);
    report.check();
}

TEST_CASE("set_delta builds Boolean members faithfully", "[codegen][differential][set_delta]") {
    // The Boolean set domains above depend on this: std::vector<bool> hands out
    // bit proxies, and set_delta once copied the proxy's bytes as the member.
    CHECK(set_delta<Bool>({false}, {}).to_string() == "{added: {False}, removed: {}}");
    CHECK(set_delta<Bool>({false}, {true}).to_string() == "{added: {False}, removed: {True}}");
    CHECK_FALSE(set_delta<Bool>({false}, {}).equals(set_delta<Bool>({true}, {})));
}

// ---- coverage ratchet ----------------------------------------------------------

namespace
{
    std::string normalise(std::string text) {
        text.erase(std::remove_if(text.begin(), text.end(), [](unsigned char c) { return std::isspace(c); }), text.end());
        return text;
    }

    /** Every domain the hgraph_std implementation sources define: each entry
        of an ``instantiate`` list as ``op<args>``, and each concrete
        ``impl fn`` as ``op(param types)``. */
    std::set<std::string> source_domains() {
        std::set<std::string> domains;
        const std::regex impl{R"((?:^|\n)impl fn (\w+)\(([^)]*)\))"};
        for (const auto &entry : std::filesystem::directory_iterator{HGL_STD_IMPL_DIR}) {
            if (entry.path().extension() != ".hgl") { continue; }
            std::ifstream file{entry.path()};
            std::stringstream buffer;
            buffer << file.rdbuf();
            const std::string text = buffer.str();
            std::istringstream lines{text};
            std::string line;
            std::string pending;
            const auto flush_instantiate = [&] {
                if (pending.empty()) { return; }
                const std::string body = normalise(pending.substr(std::string{"instantiate"}.size()));
                int depth = 0;
                std::string current;
                for (const char c : body) {
                    if (c == '<') { ++depth; }
                    if (c == '>') { --depth; }
                    if (c == ',' && depth == 0) {
                        domains.insert(current);
                        current.clear();
                        continue;
                    }
                    current += c;
                }
                if (!current.empty()) { domains.insert(current); }
                pending.clear();
            };
            while (std::getline(lines, line)) {
                if (line.rfind("instantiate", 0) == 0) {
                    flush_instantiate();
                    pending = line;
                    continue;
                }
                if (!pending.empty() && !line.empty() && std::isspace(static_cast<unsigned char>(line.front()))) {
                    pending += " " + line;
                    continue;
                }
                flush_instantiate();
            }
            flush_instantiate();
            for (auto it = std::sregex_iterator(text.begin(), text.end(), impl); it != std::sregex_iterator(); ++it) {
                const auto &match = *it;
                {
                    std::string types;
                    std::istringstream params{match[2].str()};
                    std::string param;
                    while (std::getline(params, param, ',')) {
                        const auto colon = param.find(':');
                        if (colon == std::string::npos) { continue; }
                        std::string type = param.substr(colon + 1);
                        if (const auto eq = type.find('='); eq != std::string::npos) { type = type.substr(0, eq); }
                        if (!types.empty()) { types += ","; }
                        types += normalise(type);
                    }
                    domains.insert(match[1].str() + "(" + types + ")");
                }
            }
        }
        return domains;
    }

    /** Domains deliberately outside this file, each with the reason. A domain
        listed here that the sources no longer define fails the ratchet too,
        so the list cannot rot. */
    const std::map<std::string, std::string> &excluded() {
        static const std::map<std::string, std::string> reasons{
            {"pass_through<_>", "generic structural forwarding; covered by the pass_through_* hgraph_std suites"},
            {"replay<_>", "test harness source over a const list; no native counterpart with this signature"},
            {"record<_>", "test harness sink into global state; no native counterpart with this signature"},
            {"len_<_,_>", "collection length; see generated_standard_library_tests.cpp"},
            {"len_<_>", "collection length; see generated_standard_library_tests.cpp"},
            {"is_empty<_,_>", "collection emptiness; see generated_standard_library_tests.cpp"},
            {"is_empty<_>", "collection emptiness; see generated_standard_library_tests.cpp"},
            {"len_(str)", "string length; see generated_standard_library_tests.cpp"},
            {"is_empty(str)", "string emptiness; see generated_standard_library_tests.cpp"},
            {"contains_<_>", "collection membership; see generated_catalogue_tests.cpp (membership)"},
            {"index_of<_>", "fixed-list search; see generated_catalogue_tests.cpp (membership)"},
            {"null_sink(signal)", "sink without output"},
            {"print_(str)", "console side effect; output text is not observable here"},
            {"log_(str,i64)", "logger side effect; output text is not observable here"},
            {"assert_(bool,str)", "raises by design; checked in hgraph_std tests/control.hgl"},
            {"tick_count(signal)", "internal signal counter; no public native operator"},
            {"last_modified_time(signal)", "engine-time observation; see generated_catalogue_tests.cpp"},
            {"last_modified_date(signal)", "engine-time observation; see generated_catalogue_tests.cpp"},
            {"schedule(duration,bool,i64,bool)", "source with checked arguments; see generated_catalogue_tests.cpp"},
            {"substr(str,i64,i64)", "see generated_catalogue_tests.cpp; generated byte offsets are a follow-up"},
            {"eq_epsilon(f64,f64,f64)", "tolerance parameter; see generated_catalogue_tests.cpp"},
            {"eq_epsilon(i64,f64,f64)", "tolerance parameter; see generated_catalogue_tests.cpp"},
            {"eq_epsilon(f64,i64,f64)", "tolerance parameter; see generated_catalogue_tests.cpp"},
            {"nothing<bool>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"nothing<i64>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"nothing<f64>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"nothing<str>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"nothing<date>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"nothing<time>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"nothing<datetime>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"nothing<duration>", "source with an output type only; never ticks (tests/control.hgl)"},
            {"default_ref<bool>", "internal reference node behind default; no native operator (compared through default)"},
            {"default_ref<i64>", "internal reference node behind default; no native operator (compared through default)"},
            {"default_ref<f64>", "internal reference node behind default; no native operator (compared through default)"},
            {"default_ref<str>", "internal reference node behind default; no native operator (compared through default)"},
            {"default_ref<date>", "internal reference node behind default; no native operator (compared through default)"},
            {"default_ref<time>", "internal reference node behind default; no native operator (compared through default)"},
            {"default_ref<datetime>", "internal reference node behind default; no native operator (compared through default)"},
            {"default_ref<duration>", "internal reference node behind default; no native operator (compared through default)"},
            {"debug_print(str,bool,bool,i64)", "console side effect; output text is not observable here"},
            {"debug_print(str,i64,bool,i64)", "console side effect; output text is not observable here"},
            {"debug_print(str,f64,bool,i64)", "console side effect; output text is not observable here"},
            {"debug_print(str,str,bool,i64)", "console side effect; output text is not observable here"},
            {"debug_print(str,date,bool,i64)", "console side effect; output text is not observable here"},
            {"debug_print(str,time,bool,i64)", "console side effect; output text is not observable here"},
            {"debug_print(str,datetime,bool,i64)", "console side effect; output text is not observable here"},
            {"debug_print(str,duration,bool,i64)", "console side effect; output text is not observable here"},
        };
        return reasons;
    }
}  // namespace

TEST_CASE("differential: every hgraph_std scalar domain is compared or excluded with a reason", "[codegen][differential]") {
    // The differential test cases above register their domains as they run;
    // Catch2 runs test cases in declaration order, so this case runs last.
    if (covered().empty()) { SKIP("run the whole file: the differential cases register what they compare"); }
    const auto sources = source_domains();
    REQUIRE_FALSE(sources.empty());
    std::vector<std::string> missing;
    std::vector<std::string> stale;
    for (const auto &domain : sources) {
        if (!covered().contains(domain) && !excluded().contains(domain)) { missing.push_back(domain); }
    }
    for (const auto &[domain, reason] : excluded()) {
        if (!sources.contains(domain)) { stale.push_back(domain); }
    }
    for (const auto &domain : covered()) {
        if (!sources.contains(domain)) { stale.push_back(domain + " (compared here, absent from the sources)"); }
    }
    std::ostringstream text;
    for (const auto &domain : missing) { text << "\n  not compared: " << domain; }
    for (const auto &domain : stale) { text << "\n  stale entry:  " << domain; }
    INFO("hgraph_std defines " << sources.size() << " domains; compared " << covered().size() << ", excluded "
                               << excluded().size() << text.str());
    CHECK(missing.empty());
    CHECK(stale.empty());
}
