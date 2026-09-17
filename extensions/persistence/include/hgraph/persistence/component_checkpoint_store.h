#ifndef HGRAPH_PERSISTENCE_COMPONENT_CHECKPOINT_STORE_H
#define HGRAPH_PERSISTENCE_COMPONENT_CHECKPOINT_STORE_H

#include <hgraph/persistence/export.h>
#include <hgraph/persistence/frame_store.h>
#include <hgraph/runtime/component_checkpoint.h>

#include <optional>
#include <string>
#include <string_view>

namespace hgraph::persistence
{
    /** Immutable, whole-component completed-day images.
     *
     * One checkpoint is one atomically published FrameStore object. Callers
     * select an explicit predecessor key and a new key for each day; there is
     * no mutable latest pointer or discovery by directory listing. A key that
     * exists is complete. Memory locations are useful for tests; a local or
     * S3 location retains images across process restarts.
     */
    class HGRAPH_PERSISTENCE_CLASS_EXPORT ComponentCheckpointStore final
    {
      public:
        explicit ComponentCheckpointStore(store::FrameStoreConfig config = {});

        [[nodiscard]] bool contains(std::string_view key) const;
        [[nodiscard]] ComponentCheckpoint read(std::string_view key) const;
        void write(std::string_view key, const ComponentCheckpoint &checkpoint,
                   std::optional<std::string_view> predecessor = {}) const;

      private:
        store::FrameStore frames_;
    };

    /** Configure one component's completed-day recovery for the next run.
     *
     * The core captures before stop and calls the commit callback only after
     * the bounded run and ordinary stop both succeed. Encoding is completed
     * before the store publishes the single immutable object. Restore uses
     * exactly restore_key and refuses missing or incompatible images.
     */
    HGRAPH_PERSISTENCE_EXPORT void configure_component_recovery(
        GlobalStateView state, const ComponentCheckpointStore &store,
        std::string component_id, std::string checkpoint_key,
        std::optional<std::string> restore_key = {}, std::string revision = "1");
}

#endif
