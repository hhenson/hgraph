#include <hgraph/persistence/object_store.h>
#include <hgraph/util/environment.h>

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <string>
#include <thread>
#include <type_traits>

#if !defined(_WIN32)
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace hgraph::persistence::store;

namespace
{
    [[nodiscard]] ObjectBytes bytes(std::string_view value)
    {
        const auto span = std::as_bytes(std::span{value.data(), value.size()});
        return {span.begin(), span.end()};
    }

    [[nodiscard]] std::string text(const StoredObject &object)
    {
        return {reinterpret_cast<const char *>(object.data.data()), object.data.size()};
    }

    class TempStoreDir final
    {
      public:
        explicit TempStoreDir(std::string_view name)
        {
            static std::atomic<std::uint64_t> sequence{0};
            base_ = std::filesystem::temp_directory_path() /
                    ("hgraph_object_store_" + std::string{name} + "_" +
                     std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)));
            std::filesystem::remove_all(base_);
            std::filesystem::create_directories(base_);
        }
        TempStoreDir(const TempStoreDir &) = delete;
        TempStoreDir &operator=(const TempStoreDir &) = delete;
        ~TempStoreDir() { std::filesystem::remove_all(base_); }

        [[nodiscard]] std::string root() const { return (base_ / "objects").string(); }

        [[nodiscard]] std::filesystem::path root_path() const { return base_ / "objects"; }

      private:
        std::filesystem::path base_{};
    };

    struct ProbeStore
    {
        std::optional<StoredObject> object{};
    };

    [[nodiscard]] ObjectStoreOps probe_ops()
    {
        return ObjectStoreOps{
            [](void *context, std::string_view, std::span<const std::byte> data) {
                auto &probe = *static_cast<ProbeStore *>(context);
                probe.object = StoredObject{ObjectBytes{data.begin(), data.end()}, "probe"};
                return ImmutableWriteResult{ImmutableWriteStatus::Created, "probe"};
            },
            [](void *context, std::string_view) {
                return static_cast<ProbeStore *>(context)->object;
            },
            [](void *, std::string_view, std::optional<std::string_view>, std::size_t) {
                return ObjectListPage{};
            },
            [](void *context, std::string_view, std::optional<std::string_view>,
               std::span<const std::byte> desired) {
                auto &probe = *static_cast<ProbeStore *>(context);
                probe.object = StoredObject{ObjectBytes{desired.begin(), desired.end()}, "cas"};
                return CompareExchangeResult{true, probe.object};
            },
            [](void *context) { static_cast<ProbeStore *>(context)->object.reset(); },
        };
    }

    void check_store_contract(ObjectStore store)
    {
        const auto first = bytes("first");
        const auto other = bytes("other");

        CHECK_FALSE(store.get("missing"));
        const auto created = store.put_immutable("values/item", first);
        CHECK(created.status == ImmutableWriteStatus::Created);
        CHECK_FALSE(created.version_token.empty());
        REQUIRE(store.get("values/item"));
        CHECK(text(*store.get("values/item")) == "first");

        const auto unchanged = store.put_immutable("values/item", first);
        CHECK(unchanged.status == ImmutableWriteStatus::Unchanged);
        CHECK(unchanged.version_token == created.version_token);
        const auto conflict = store.put_immutable("values/item", other);
        CHECK(conflict.status == ImmutableWriteStatus::Conflict);
        CHECK(conflict.version_token == created.version_token);
        CHECK(text(*store.get("values/item")) == "first");

        const auto empty = ObjectBytes{};
        CHECK(store.put_immutable("values/empty", empty).status == ImmutableWriteStatus::Created);
        REQUIRE(store.get("values/empty"));
        CHECK(store.get("values/empty")->data.empty());

        const auto initial_ref = store.compare_exchange_ref("refs/latest", std::nullopt, first);
        REQUIRE(initial_ref.exchanged);
        REQUIRE(initial_ref.current);
        CHECK(text(*initial_ref.current) == "first");

        const auto duplicate_create =
            store.compare_exchange_ref("refs/latest", std::nullopt, other);
        CHECK_FALSE(duplicate_create.exchanged);
        REQUIRE(duplicate_create.current);
        CHECK(text(*duplicate_create.current) == "first");

        const auto advanced =
            store.compare_exchange_ref("refs/latest", initial_ref.current->version_token, other);
        REQUIRE(advanced.exchanged);
        REQUIRE(advanced.current);
        CHECK(text(*advanced.current) == "other");

        const auto stale =
            store.compare_exchange_ref("refs/latest", initial_ref.current->version_token, first);
        CHECK_FALSE(stale.exchanged);
        REQUIRE(stale.current);
        CHECK(text(*stale.current) == "other");

        CHECK(store.put_immutable("values/a", bytes("a")).status == ImmutableWriteStatus::Created);
        CHECK(store.put_immutable("values/b", bytes("b")).status == ImmutableWriteStatus::Created);
        CHECK(store.put_immutable("values/c", bytes("c")).status == ImmutableWriteStatus::Created);

        const auto first_page = store.list("values/", {}, 2);
        REQUIRE(first_page.objects.size() == 2);
        CHECK(first_page.objects[0].key == "values/a");
        CHECK(first_page.objects[1].key == "values/b");
        REQUIRE(first_page.next_start_after);
        const auto second_page = store.list("values/", *first_page.next_start_after, 2);
        REQUIRE(second_page.objects.size() == 2);
        CHECK(second_page.objects[0].key == "values/c");
        CHECK(second_page.objects[1].key == "values/empty");
        REQUIRE(second_page.next_start_after);
        const auto third_page = store.list("values/", *second_page.next_start_after, 2);
        REQUIRE(third_page.objects.size() == 1);
        CHECK(third_page.objects[0].key == "values/item");
        CHECK_FALSE(third_page.next_start_after);

        store.clear();
        CHECK_FALSE(store.get("values/item"));
        CHECK(store.list("", {}, 10).objects.empty());
    }

    void check_concurrent_winners(ObjectStore store, std::string_view prefix)
    {
        constexpr std::size_t contenders = 8;
        const std::string     object_key = std::string{prefix} + "/object";
        const std::string     ref_key = std::string{prefix} + "/ref";

        std::array<std::future<ImmutableWriteResult>, contenders> creates;
        for (std::size_t index = 0; index < contenders; ++index)
        {
            creates[index] = std::async(std::launch::async, [store, object_key, index] {
                return store.put_immutable(object_key, bytes(std::to_string(index)));
            });
        }
        std::size_t created = 0;
        for (auto &future : creates)
        {
            created += future.get().status == ImmutableWriteStatus::Created ? 1 : 0;
        }
        CHECK(created == 1);

        const auto seed = store.compare_exchange_ref(ref_key, {}, bytes("seed"));
        REQUIRE(seed.exchanged);
        REQUIRE(seed.current);
        const auto expected = seed.current->version_token;

        std::array<std::future<CompareExchangeResult>, contenders> exchanges;
        for (std::size_t index = 0; index < contenders; ++index)
        {
            exchanges[index] = std::async(std::launch::async, [store, ref_key, expected, index] {
                return store.compare_exchange_ref(ref_key, expected, bytes(std::to_string(index)));
            });
        }
        std::size_t exchanged = 0;
        for (auto &future : exchanges)
        {
            exchanged += future.get().exchanged ? 1 : 0;
        }
        CHECK(exchanged == 1);
    }
}  // namespace

TEST_CASE("object store: public facade owns an erased strategy and has a "
          "canonical empty table")
{
    static_assert(!std::is_polymorphic_v<ObjectStore>);

    auto                      context = std::make_shared<ProbeStore>();
    const std::weak_ptr<void> lifetime = context;
    // A downstream strategy may build its operations table on demand. The
    // handle must own that table rather than borrow this temporary.
    ObjectStore store{context, probe_ops()};
    ObjectStore               copy = store;
    context.reset();

    CHECK(store.put_immutable("value", bytes("one")).status == ImmutableWriteStatus::Created);
    REQUIRE(copy.get("value"));
    CHECK(text(*copy.get("value")) == "one");

    ObjectStore moved = std::move(store);
    CHECK_FALSE(store);
    CHECK_FALSE(store.get("value"));
    CHECK(store.list("").objects.empty());
    CHECK_NOTHROW(store.clear());
    CHECK_THROWS_AS(store.put_immutable("value", bytes("two")), std::logic_error);
    CHECK_THROWS_AS(store.compare_exchange_ref("value", {}, bytes("two")), std::logic_error);

    copy.reset();
    CHECK_FALSE(lifetime.expired());
    moved.reset();
    CHECK(lifetime.expired());

    CHECK_THROWS_AS((ObjectStore{std::shared_ptr<void>{}, probe_ops()}), std::invalid_argument);
    const ObjectStoreOps incomplete{};
    CHECK_THROWS_AS((ObjectStore{std::make_shared<ProbeStore>(), incomplete}),
                    std::invalid_argument);
}

TEST_CASE("object store: validation and typed absence are backend independent")
{
    auto store = make_object_store(ObjectStoreConfig{});
    CHECK_THROWS_AS(store.get("../escape"), std::invalid_argument);
    CHECK_THROWS_AS(store.put_immutable("trailing/", bytes("bad")), std::invalid_argument);
    CHECK_THROWS_AS(store.list("/absolute"), std::invalid_argument);
    CHECK_THROWS_AS(store.list("", {}, 0), std::invalid_argument);
    CHECK_FALSE(store.get("absent"));
    CHECK_THROWS_AS(make_object_store(ObjectStoreConfig{LocalLocation{
                        std::filesystem::temp_directory_path().root_path().string()}}),
                    std::invalid_argument);

    TempStoreDir local_dir{"failure"};
    auto         local = make_object_store(ObjectStoreConfig{LocalLocation{local_dir.root()}});
    std::filesystem::create_directories(local_dir.root_path() / "not-an-object");
    CHECK_THROWS_AS(local.get("not-an-object"), ObjectStoreError);
}

TEST_CASE("object store: memory backend implements immutable objects, CAS, and "
          "paging")
{
    check_store_contract(make_object_store(ObjectStoreConfig{}));
}

TEST_CASE("object store: memory immutable creation and reference CAS have one winner")
{
    check_concurrent_winners(make_object_store(ObjectStoreConfig{}), "race");
}

TEST_CASE("object store: local backend persists across independently "
          "constructed handles")
{
    TempStoreDir      dir{"local"};
    ObjectStoreConfig config{LocalLocation{dir.root()}};
    auto              first = make_object_store(config);
    auto              second = make_object_store(config);

    CHECK(first.put_immutable("shared/value", bytes("durable")).status ==
          ImmutableWriteStatus::Created);
    REQUIRE(second.get("shared/value"));
    CHECK(text(*second.get("shared/value")) == "durable");
    check_store_contract(std::move(first));
}

TEST_CASE("object store: local immutable creation and reference CAS have one "
          "winner")
{
    TempStoreDir          dir{"race"};
    ObjectStoreConfig     config{LocalLocation{dir.root()}};
    constexpr std::size_t contenders = 16;

    std::array<std::future<ImmutableWriteResult>, contenders> creates;
    for (std::size_t index = 0; index < contenders; ++index)
    {
        creates[index] = std::async(std::launch::async, [config, index] {
            auto store = make_object_store(config);
            return store.put_immutable("race/object", bytes(std::to_string(index)));
        });
    }
    std::size_t created = 0;
    for (auto &future : creates)
    {
        created += future.get().status == ImmutableWriteStatus::Created ? 1 : 0;
    }
    CHECK(created == 1);

    auto seed_store = make_object_store(config);
    auto seed = seed_store.compare_exchange_ref("race/ref", {}, bytes("seed"));
    REQUIRE(seed.exchanged);
    REQUIRE(seed.current);
    const auto expected = seed.current->version_token;

    std::array<std::future<CompareExchangeResult>, contenders> exchanges;
    for (std::size_t index = 0; index < contenders; ++index)
    {
        exchanges[index] = std::async(std::launch::async, [config, expected, index] {
            auto store = make_object_store(config);
            return store.compare_exchange_ref("race/ref", expected, bytes(std::to_string(index)));
        });
    }
    std::size_t exchanged = 0;
    for (auto &future : exchanges)
    {
        exchanged += future.get().exchanged ? 1 : 0;
    }
    CHECK(exchanged == 1);
}

#if !defined(_WIN32)
TEST_CASE("object store: local immutable creation is atomic across processes")
{
    TempStoreDir                  dir{"process_race"};
    const auto                    root = dir.root();
    constexpr int                 contenders = 8;
    std::array<pid_t, contenders> children{};

    for (int index = 0; index < contenders; ++index)
    {
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0)
        {
            try
            {
                auto       store = make_object_store(ObjectStoreConfig{LocalLocation{root}});
                const auto result =
                    store.put_immutable("race/process", bytes(std::to_string(index)));
                ::_exit(result.status == ImmutableWriteStatus::Created ||
                                result.status == ImmutableWriteStatus::Conflict
                            ? 0
                            : 2);
            }
            catch (...)
            {
                ::_exit(3);
            }
        }
        children[static_cast<std::size_t>(index)] = child;
    }

    for (const auto child : children)
    {
        int status = 0;
        REQUIRE(::waitpid(child, &status, 0) == child);
        CHECK(WIFEXITED(status));
        CHECK(WEXITSTATUS(status) == 0);
    }
    auto store = make_object_store(ObjectStoreConfig{LocalLocation{root}});
    REQUIRE(store.get("race/process"));
}

TEST_CASE("object store: local reference CAS has one winner across processes")
{
    TempStoreDir dir{"process_cas"};
    const auto   root = dir.root();
    auto         store = make_object_store(ObjectStoreConfig{LocalLocation{root}});
    const auto   seed = store.compare_exchange_ref("race/ref", {}, bytes("seed"));
    REQUIRE(seed.exchanged);
    REQUIRE(seed.current);
    const std::string expected = seed.current->version_token;
    store.reset();

    constexpr int                 contenders = 8;
    std::array<pid_t, contenders> children{};
    for (int index = 0; index < contenders; ++index)
    {
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0)
        {
            try
            {
                auto       child_store = make_object_store(ObjectStoreConfig{LocalLocation{root}});
                const auto result = child_store.compare_exchange_ref("race/ref", expected,
                                                                     bytes(std::to_string(index)));
                ::_exit(result.exchanged ? 0 : 1);
            }
            catch (...)
            {
                ::_exit(3);
            }
        }
        children[static_cast<std::size_t>(index)] = child;
    }

    int winners = 0;
    for (const auto child : children)
    {
        int status = 0;
        REQUIRE(::waitpid(child, &status, 0) == child);
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) != 3);
        winners += WEXITSTATUS(status) == 0 ? 1 : 0;
    }
    CHECK(winners == 1);
}

TEST_CASE("object store: a crashed local writer never exposes a partial object")
{
    using namespace std::chrono_literals;

    TempStoreDir dir{"crash"};
    const auto   root = dir.root_path();
    auto         prepared = make_object_store(ObjectStoreConfig{LocalLocation{root.string()}});
    prepared.reset();

    constexpr std::size_t payload_size = 32U * 1024U * 1024U;
    const ObjectBytes     payload(payload_size, std::byte{0x5a});
    const auto staging = root.parent_path() / ("." + root.filename().string() + ".hgraph-staging");
    const auto destination = root / "crash" / "object";

    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0)
    {
        try
        {
            auto child_store = make_object_store(ObjectStoreConfig{LocalLocation{root.string()}});
            (void)child_store.put_immutable("crash/object", payload);
            ::_exit(0);
        }
        catch (...)
        {
            ::_exit(3);
        }
    }

    int  status = 0;
    bool reaped = false;
    for (int attempt = 0; attempt < 5000; ++attempt)
    {
        std::error_code error;
        const bool      destination_visible = std::filesystem::exists(destination, error);
        REQUIRE_FALSE(error);
        const bool staging_visible = std::filesystem::exists(staging, error) &&
                                     std::filesystem::directory_iterator{staging, error} !=
                                         std::filesystem::directory_iterator{};
        REQUIRE_FALSE(error);
        if (destination_visible || staging_visible)
        {
            (void)::kill(child, SIGKILL);
            break;
        }
        const auto waited = ::waitpid(child, &status, WNOHANG);
        REQUIRE(waited >= 0);
        if (waited == child)
        {
            reaped = true;
            break;
        }
        std::this_thread::sleep_for(1ms);
    }
    if (!reaped)
    {
        (void)::kill(child, SIGKILL);
        REQUIRE(::waitpid(child, &status, 0) == child);
    }

    auto       store = make_object_store(ObjectStoreConfig{LocalLocation{root.string()}});
    const auto object = store.get("crash/object");
    if (object)
    {
        CHECK(object->data == payload);
    }
}
#endif

TEST_CASE("object store: S3 uses conditional writes, ETags, and ordered discovery", "[.s3]")
{
    const auto endpoint = hgraph::environment_variable("HGRAPH_S3_TEST_ENDPOINT");
    if (!endpoint)
    {
        SKIP("set HGRAPH_S3_TEST_ENDPOINT to run the object-store S3 contract");
    }

    S3Location location;
    location.bucket =
        hgraph::environment_variable("HGRAPH_S3_TEST_BUCKET").value_or("hgraph-test");
    location.prefix = "/object-store/";
    location.region = "us-east-1";
    location.endpoint_override = *endpoint;
    if (const auto key = hgraph::environment_variable("AWS_ACCESS_KEY_ID"))
    {
        const auto secret = hgraph::environment_variable("AWS_SECRET_ACCESS_KEY");
        REQUIRE(secret.has_value());
        location.credentials.source = Credentials::Explicit{*key, *secret, {}};
    }

    auto store = make_object_store(ObjectStoreConfig{location});
    store.clear();
    check_store_contract(store);
    check_concurrent_winners(store, "race");
    store.clear();
    store.reset();

    auto missing_bucket = location;
    missing_bucket.bucket += "-missing";
    auto invalid_store = make_object_store(ObjectStoreConfig{missing_bucket});
    CHECK_THROWS_AS(invalid_store.get("absent"), ObjectStoreError);
    invalid_store.reset();
}
