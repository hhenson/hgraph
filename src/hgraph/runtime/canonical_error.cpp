#include <hgraph/runtime/canonical_error.h>
#include <hgraph/runtime/node.h>
namespace hgraph {
    template HGRAPH_EXPORT const MemoryUtils::StoragePlan &MemoryUtils::plan_for<CanonicalError>() noexcept;
    template HGRAPH_EXPORT const ValueOps &ops_for<CanonicalError>() noexcept;
    template HGRAPH_EXPORT const MemoryUtils::StoragePlan &MemoryUtils::plan_for<ErrorContext>() noexcept;
    template HGRAPH_EXPORT const ValueOps &ops_for<ErrorContext>() noexcept;

    namespace {
        std::optional<CanonicalError> original_failure(const std::exception &failure) {
            if (const auto *known = dynamic_cast<const ExecutionFailure *>(&failure)) { return known->error(); }
            const auto *nested = dynamic_cast<const std::nested_exception *>(&failure);
            if (nested && nested->nested_ptr()) {
                try { std::rethrow_exception(nested->nested_ptr()); }
                catch (const std::exception &cause) { return original_failure(cause); }
                catch (...) {}
            }
            return std::nullopt;
        }
    }
    FailurePhase execution_failure_phase(const std::exception &failure) noexcept {
        if (const auto *known = dynamic_cast<const ExecutionFailure *>(&failure)) { return known->active_phase(); }
        const auto *nested = dynamic_cast<const std::nested_exception *>(&failure);
        if (nested && nested->nested_ptr()) {
            try { std::rethrow_exception(nested->nested_ptr()); }
            catch (const std::exception &cause) { return execution_failure_phase(cause); }
            catch (...) {}
        }
        return FailurePhase::Evaluate;
    }
    CanonicalError canonicalize_failure(const NodeView &node, DateTime time, std::string phase, const std::exception &failure) {
        CanonicalError error;
        if (auto original = original_failure(failure)) { error = std::move(*original); }
        else { error.message = failure.what(); }
        if (!error.context) {
            const auto *schema = node.schema();
            std::string identity = schema && schema->display_name ? schema->display_name : "node";
            identity += "@" + std::to_string(node.instance_identity());
            error.context = ErrorContext{std::move(identity), std::move(phase), time};
        }
        return error;
    }
    const ValueTypeMetaData *canonical_error_value_meta() {
        return TypeRegistry::instance().register_scalar<CanonicalError>("hgl.error");
    }
    const ValueTypeMetaData *error_context_value_meta() {
        return TypeRegistry::instance().register_scalar<ErrorContext>("hgl.error.context");
    }
}
