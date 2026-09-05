#ifndef HGRAPH_RUNTIME_GLOBAL_STATE_H
#define HGRAPH_RUNTIME_GLOBAL_STATE_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/value/value.h>

#include <string>
#include <string_view>
#include <memory>
#include <vector>

namespace hgraph
{
    /**
     * Borrowing **view** over a graph's ``GlobalState`` — the access surface and
     * the node-level injectable. It is the value/view split applied to global
     * state: the owning ``GlobalState`` holds the data on the root graph /
     * builder; callers (and nodes, via the ``GlobalStateView`` selector) receive
     * this view to read and write it. Non-owning: it borrows the owner's value,
     * which must outlive the view.
     *
     * The store is a mutable ``Map<string, Any>``; values are boxed in ``Any`` on
     * the way in (``set`` assigns in place — no temporary) and unwrapped on the
     * way out (``get`` returns the contained value directly).
     */
    class HGRAPH_CLASS_EXPORT GlobalStateView
    {
      public:
        GlobalStateView() noexcept = default;
        explicit GlobalStateView(Value &map) noexcept : map_(&map) {}

        [[nodiscard]] bool valid() const noexcept { return map_ != nullptr; }

        /** Number of keys currently stored. */
        [[nodiscard]] std::size_t size() const;
        /** True when ``key`` is present. */
        [[nodiscard]] bool contains(std::string_view key) const;

        /**
         * Read the value stored under ``key`` (the contained value, unwrapped
         * from its ``Any`` box). Returns an **invalid** ``ValueView`` when absent.
         *
         * The store is mutable by definition, so the read honours the stored
         * value's own mutability: a value boxed as **mutable** (e.g. a mutable
         * ``List``/``Map``) comes back as a **writable** view that can be mutated
         * in place; an **immutable** value comes back read-only (its ops refuse
         * ``begin_mutation``).
         */
        [[nodiscard]] ValueView get(std::string_view key) const;

        /** Typed read; throws when absent or the type does not match. */
        template <typename T>
        [[nodiscard]] const T &get_as(std::string_view key) const
        {
            return get(key).template checked_as<T>();
        }

        /** Insert or replace ``key`` with a copy of ``value`` (assigned in place). */
        void set(std::string_view key, const ValueView &value) const;
        void set(std::string_view key, const Value &value) const;
        /** Insert or replace ``key`` by MOVING ``value`` into the store (no copy). */
        void set(std::string_view key, Value &&value) const;

        /** Remove ``key``; returns whether a key was removed. */
        bool erase(std::string_view key) const;

        /** Replace this store with a copy of ``other``. */
        void copy_from(const GlobalStateView &other) const;

        /** The underlying ``Map<string, Any>`` value. */
        [[nodiscard]] const Value &as_value() const noexcept { return *map_; }

      private:
        Value *map_{nullptr};  // borrowed; owned by a GlobalState
    };

    /**
     * Owning ``GlobalState`` — a mutable, heterogeneous ``string -> value`` store
     * shared across a graph and the first **injectable**. It is created by the
     * top-level graph builder and carried (copied) onto the root graph, so the
     * same data is available at wiring time and at run time. It holds the value;
     * callers obtain a :cpp:class:`GlobalStateView` (via ``view()``,
     * ``GraphBuilder::global_state()`` or ``GraphView::global_state()``) to use
     * it — usage is explicit, a node only sees it when it declares it.
     *
     * Backed by a value-layer mutable ``Map<string, Any>`` ``Value`` — so it is a
     * single type-erased value: copyable, and bindable to Python.
     */
    class HGRAPH_CLASS_EXPORT GlobalState
    {
      public:
        /** Construct an empty store (an empty mutable ``Map<string, Any>``). */
        GlobalState();

        GlobalState(const GlobalState &)                = default;
        GlobalState &operator=(const GlobalState &)     = default;
        GlobalState(GlobalState &&) noexcept            = default;
        GlobalState &operator=(GlobalState &&) noexcept = default;
        ~GlobalState()                                  = default;

        /** A view over this store. */
        [[nodiscard]] GlobalStateView view() noexcept { return GlobalStateView{map_}; }

        /** The underlying ``Map<string, Any>`` value. */
        [[nodiscard]] const Value &as_value() const noexcept { return map_; }

      private:
        Value map_;  // mutable Map<string, Any>
    };

    /**
     * A detachable binding to a LIVE seed state. Every holder -- a top-level
     * wiring, the child wirings it spawns, a prepared ``lower`` execution --
     * reads the seed through the shared binding; the owner (a
     * ``GlobalContext`` when it exits, or the top-level wiring an explicit
     * seed was handed to when it releases) DETACHES it, after which every
     * holder sees no seed and nothing can dereference a dead state. No
     * holder ever copies the raw pointer out.
     */
    class HGRAPH_CLASS_EXPORT GlobalSeedBinding
    {
      public:
        explicit GlobalSeedBinding(GlobalState *state) noexcept : state_(state) {}
        [[nodiscard]] GlobalState *state() const noexcept { return state_; }
        void detach() noexcept { state_ = nullptr; }

      private:
        GlobalState *state_{nullptr};
    };
    using GlobalSeed = std::shared_ptr<GlobalSeedBinding>;

    /**
     * C++ authoring shorthand that selects the LIVE global state for the
     * wirings constructed while it is active (lifecycle ruling 2026-07-27;
     * no-thread-locals ruling 2026-09-05). The build is single-threaded, so
     * the active context is one process-wide slot, like the wiring-time
     * context stack on the operator registry -- never a thread-local. A
     * ``Wiring`` takes the context's ``GlobalSeed`` at construction and
     * reads and writes the state through that binding for the duration of
     * the wiring; nothing copies it. The wiring end (graph build) FIXES the
     * seed: the state is copied then, as it stands, into the graph as its
     * initial state; the run works on that isolation copy (global to the
     * runtime graph and all nested graphs) and results copy back at run
     * end. The selected state must span the wiring process: a context that
     * exits early detaches the binding it handed out, so every wiring and
     * child seeded from it falls back to its own store and a top-level
     * build fails loudly rather than dereferencing a dead state. Language
     * bridges do not use the context: they hand the state to
     * ``Wiring(GlobalState &)`` directly, which is what lets distinct runs
     * proceed on distinct threads.
     */
    class HGRAPH_CLASS_EXPORT GlobalContext
    {
      public:
        GlobalContext();
        explicit GlobalContext(GlobalState &state);

        GlobalContext(const GlobalContext &) = delete;
        GlobalContext &operator=(const GlobalContext &) = delete;
        GlobalContext(GlobalContext &&) = delete;
        GlobalContext &operator=(GlobalContext &&) = delete;
        ~GlobalContext();

        [[nodiscard]] GlobalState &state() const noexcept { return *state_; }
        /** The binding this context hands to the wirings constructed under it. */
        [[nodiscard]] const GlobalSeed &seed() const noexcept { return seed_; }
        /** The active context, or null: consulted once, when a ``Wiring`` is
            constructed, and by C++ test harnesses. */
        [[nodiscard]] static GlobalContext *active() noexcept;
        [[nodiscard]] static GlobalState *active_state() noexcept;

      private:
        void activate();

        GlobalState  owned_state_{};
        GlobalState *state_{nullptr};
        GlobalSeed   seed_{};
    };
}  // namespace hgraph

#endif  // HGRAPH_RUNTIME_GLOBAL_STATE_H
