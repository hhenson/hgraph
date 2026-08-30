#include <hgraph/fabric/history.h>

#include <hgraph/fabric/keys.h>
#include <hgraph/fabric/metadata_codec.h>
#include <hgraph/fabric/value_builders.h>

#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace hgraph::fabric
{
    std::optional<Frame> load_data(const FabricConfig &config, Str data_id,
                                   DateTime as_of)
    {
        require_valid_config(config);
        require_data_id(data_id);
        if (as_of <= MIN_DT)
        {
            throw std::invalid_argument("fabric load as_of must be a real instant");
        }
        if (as_of.time_since_epoch().count() <= 0)
        {
            return std::nullopt;
        }

        const std::string category = as_of_key_prefix(config.prefix, data_id);
        const std::string prefix = category + "/";
        const std::string target = as_of_key(config.prefix, data_id, as_of);
        std::optional<std::string> selected;
        std::optional<DateTime> selected_as_of;
        std::optional<std::string> cursor;

        for (;;)
        {
            const auto page = config.objects.list(
                prefix,
                cursor.has_value() ? std::optional<std::string_view>{*cursor} : std::nullopt,
                1000U);
            bool reached_cutoff = false;
            for (const auto &object : page.objects)
            {
                if (!object.key.starts_with(prefix))
                {
                    throw std::runtime_error("fabric as-of listing escaped its data id");
                }
                const Int ordinal = decode_fabric_ordinal(
                    std::string_view{object.key}.substr(prefix.size()));
                if (object.key > target)
                {
                    reached_cutoff = true;
                    break;
                }
                selected = object.key;
                selected_as_of = DateTime{TimeDelta{ordinal}};
            }
            if (reached_cutoff || !page.next_start_after.has_value())
            {
                break;
            }
            cursor = page.next_start_after;
        }

        if (!selected.has_value() || !selected_as_of.has_value())
        {
            return std::nullopt;
        }
        const auto reference = config.objects.get(*selected);
        if (!reference.has_value())
        {
            throw std::runtime_error("fabric as-of index disappeared during load");
        }
        const RevisionId revision_id = revision_reference_value(config.reference_codec, MetadataObjectKind::AsOf, reference->data);
        const auto stored_revision = config.objects.get(
            revision_key(config.prefix, data_id, revision_id));
        if (!stored_revision.has_value())
        {
            throw std::runtime_error("fabric as-of index references a missing revision");
        }
        const DataRevisionInput revision = data_revision_input(
            decode_data_revision(config.revision_codec, stored_revision->data)
                .view());
        if (revision.data_id != data_id || revision.revision != revision_id ||
            revision.as_of != *selected_as_of)
        {
            throw std::runtime_error("fabric as-of index references inconsistent revision metadata");
        }

        Frame frame = config.frames.read(
            data_version_key(config.prefix, data_id, revision.output_version));
        if (!frame.has_value())
        {
            throw std::runtime_error("fabric as-of revision references a missing frame");
        }
        return frame;
    }

}  // namespace hgraph::fabric
