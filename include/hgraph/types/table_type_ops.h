#ifndef HGRAPH_TYPES_TABLE_TYPE_OPS_H
#define HGRAPH_TYPES_TABLE_TYPE_OPS_H

#include <hgraph/hgraph_export.h>
#include <hgraph/types/metadata/ts_value_type_meta_data.h>
#include <hgraph/types/primitive_types.h>
#include <hgraph/types/value/value.h>
#include <hgraph/util/date_time.h>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hgraph
{
    class TableConverter;
    class TSInputView;
    class TSOutputView;
    class ValueView;

    struct TableTypeOps;
    [[nodiscard]] HGRAPH_EXPORT const TableTypeOps &empty_table_type_ops() noexcept;

    /** One cell-emission destination. The traversal resolves leaves and the
        destination decides whether they become tuple fields or Arrow builder
        appends. */
    struct TableRowSink
    {
        void *context{nullptr};
        void (*cell)(void *context, std::size_t column, const ValueView &value){nullptr};
        void (*end_row)(void *context){nullptr};
    };

    /** Read-only rows supplied to ``TableTypeOps::apply``. Implementations
        may be tuple values or stored Arrow frames; the operation sees the
        same canonical column indexes in either case. */
    struct TableRowSource
    {
        const void *context{nullptr};
        std::size_t rows{0};
        Value (*cell)(const void *context, std::size_t row, std::size_t column){nullptr};
    };

    /** Interned table plan for one resolved time-series schema. */
    struct TableLayout
    {
        static constexpr std::size_t no_column = static_cast<std::size_t>(-1);

        struct Column
        {
            std::string              name{};
            const ValueTypeMetaData *leaf{nullptr};
            std::size_t              column{no_column};
            std::vector<std::size_t> ts_path{};
            std::vector<std::size_t> value_path{};
        };

        /** A registered child strategy composed into a built-in structural
            layout.

            ``layout`` is the independently interned plan for that exact child
            schema. ``ts_path`` locates the child below the structural leaf and
            ``columns`` maps the child's canonical column indexes into this
            layout. A ``no_column`` entry suppresses an unmapped child column.
            ``presence_column`` records that the child emitted a row independently
            of whether any projected payload cell has a value.

            This is plan data only: the selected child operations are reached
            through ``layout->ops`` and no registry lookup occurs during
            evaluation. */
        struct ChildPlan
        {
            const TableLayout       *layout{nullptr};
            std::vector<std::size_t> ts_path{};
            std::vector<std::size_t> columns{};
            std::size_t              presence_column{no_column};
        };

        struct Level
        {
            const ValueTypeMetaData              *key_meta{nullptr};
            std::size_t                           removed_col{0};
            std::size_t                           first_key_col{0};
            std::vector<std::vector<std::size_t>> key_paths{};
        };

        const TSValueTypeMetaData             *ts_schema{nullptr};
        const TSValueTypeMetaData             *leaf_ts{nullptr};
        const TableTypeOps                    *ops{&empty_table_type_ops()};
        std::vector<Level>                     levels{};
        std::vector<Column>                    value_cols{};
        std::vector<ChildPlan>                 child_plans{};
        std::size_t                            value_col_start{0};
        bool                                   is_multi_row{false};
        const TableConverter                  *frame_converter{nullptr};
        std::vector<std::string>               keys{};
        std::vector<const ValueTypeMetaData *> col_metas{};
        std::vector<std::string>               partition_keys{};
        std::vector<std::string>               removed_keys{};
        std::string                            date_key{};
        std::string                            as_of_key{};
        const ValueTypeMetaData               *row_meta{nullptr};
        const ValueTypeMetaData               *rows_meta{nullptr};
        const TSValueTypeMetaData             *output_ts{nullptr};

        [[nodiscard]] bool partitioned() const noexcept { return !levels.empty(); }
        [[nodiscard]] bool multi() const noexcept { return partitioned() || is_multi_row; }
    };

    /** Per-call projection from a canonical table plan to stored columns. */
    struct TableRecordingOptions
    {
        enum class AsOf
        {
            Track,
            Omit,
            Fixed,
        };
        enum class Removes
        {
            Omit,
            Track,
        };

        AsOf                     as_of{AsOf::Track};
        std::optional<DateTime>  as_of_value{};
        Removes                  removes{Removes::Omit};
        std::string              date_key{};
        std::string              as_of_key{};
        std::vector<std::string> partition_names{};
        std::vector<std::string> removed_names{};
        std::string              frame_prefix{};
    };

    struct TableRecordingColumns
    {
        std::vector<std::string>               names{};
        std::vector<const ValueTypeMetaData *> metas{};
        std::vector<std::size_t>               source{};

        [[nodiscard]] std::size_t size() const noexcept { return names.size(); }
    };

    /** Passive table behavior selected once while building ``TableLayout``.

        ``describe`` contributes structural/value columns and may recursively
        resolve child schemas. ``emit`` and ``apply`` are parked in the plan,
        so evaluation performs no registry lookup or kind switch. Extensions
        can register an exact schema before wiring. When that schema appears
        below a built-in TSB or TSD, the layout builder composes its independently
        interned plan as a ``ChildPlan`` and evaluation delegates through the
        same operation table. The function table and its referenced code must
        have static lifetime. */
    struct TableTypeOps
    {
        using Describe = void (*)(TableLayout &layout, const TSValueTypeMetaData *schema,
                                  std::string prefix, std::vector<std::size_t> ts_path,
                                  std::size_t level_no);
        using Emit = void (*)(const TableLayout &layout, const TSInputView &ts, Int mode,
                              DateTime now, DateTime as_of, bool emit_removals,
                              const TableRowSink &sink, bool reject_empty_frames);
        using Apply = void (*)(const TableLayout &layout, const TableRowSource &source,
                               const TSOutputView &out);

        Describe describe{nullptr};
        Emit     emit{nullptr};
        Apply    apply{nullptr};
    };

    /** Register an exact-schema override. Existing cached plans are dropped. */
    HGRAPH_EXPORT void register_table_type_ops(const TSValueTypeMetaData *schema,
                                               const TableTypeOps        &ops);
    /** Resolve an exact override or the canonical built-in strategy. */
    [[nodiscard]] HGRAPH_EXPORT const TableTypeOps &table_type_ops(
        const TSValueTypeMetaData *schema);
    /** Clear exact overrides and cached strategy state on registry reset. */
    HGRAPH_EXPORT void clear_table_type_ops() noexcept;
}  // namespace hgraph

#endif  // HGRAPH_TYPES_TABLE_TYPE_OPS_H
