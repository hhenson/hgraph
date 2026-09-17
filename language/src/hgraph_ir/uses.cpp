#include "hgraph_ir/uses.h"

#include <type_traits>
#include <variant>

namespace hgl::hgraph_ir
{
    namespace
    {
        struct Collector
        {
            const Module &module;
            BindingUses  &uses;

            void block(BlockId id) {
                if (!id.valid() || id.value >= module.blocks.size()) { return; }
                const Block &node = module.blocks[id.value];
                for (StatementId statement : node.statements) { this->statement(statement); }
                value(node.tail);
            }

            void statement(StatementId id) {
                if (!id.valid() || id.value >= module.statements.size()) { return; }
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, LocalBinding> || std::is_same_v<T, StateBinding>) {
                            value(node.init);
                        } else if constexpr (std::is_same_v<T, Lifecycle>) {
                            block(node.block);
                        } else if constexpr (std::is_same_v<T, Activation>) {
                            value(node.condition);
                            block(node.block);
                        } else if constexpr (std::is_same_v<T, Traversal>) {
                            value(node.iterable);
                            block(node.block);
                        } else if constexpr (std::is_same_v<T, Assignment>) {
                            // The bare target of a plain `=` is written, not read.
                            const Value     *place     = node.place.valid() && node.place.value < module.values.size()
                                                             ? &module.values[node.place.value]
                                                             : nullptr;
                            const Reference *reference = place != nullptr ? std::get_if<Reference>(&place->node) : nullptr;
                            if (node.op == AssignOp::Assign && reference != nullptr && reference->kind == ReferenceKind::Binding &&
                                reference->binding.valid()) {
                                ++uses.references[reference->binding.value];
                            } else {
                                value(node.place);
                            }
                            value(node.value);
                        } else if constexpr (std::is_same_v<T, Return>) {
                            value(node.value);
                        } else if constexpr (std::is_same_v<T, Assert>) {
                            value(node.condition);
                        } else if constexpr (std::is_same_v<T, Evaluate>) {
                            value(node.value);
                        }
                        // Inject declares capabilities; it references nothing.
                    },
                    module.statements[id.value].node);
            }

            void value(ValueId id) {
                if (!id.valid() || id.value >= module.values.size()) { return; }
                std::visit(
                    [&](const auto &node) {
                        using T = std::decay_t<decltype(node)>;
                        if constexpr (std::is_same_v<T, Reference>) {
                            if (node.kind == ReferenceKind::Binding && node.binding.valid()) {
                                ++uses.references[node.binding.value];
                                ++uses.reads[node.binding.value];
                            }
                        } else if constexpr (std::is_same_v<T, Unary>) {
                            value(node.operand);
                        } else if constexpr (std::is_same_v<T, Binary>) {
                            value(node.lhs);
                            value(node.rhs);
                        } else if constexpr (std::is_same_v<T, Call> || std::is_same_v<T, HarnessEval>) {
                            value(node.callee);
                            for (const Argument &argument : node.arguments) { value(argument.value); }
                        } else if constexpr (std::is_same_v<T, Index>) {
                            value(node.target);
                            value(node.index);
                        } else if constexpr (std::is_same_v<T, Field>) {
                            value(node.target);
                        } else if constexpr (std::is_same_v<T, Sequence>) {
                            for (const SequenceElement &element : node.elements) {
                                value(element.key);
                                value(element.value);
                            }
                        } else if constexpr (std::is_same_v<T, Tuple>) {
                            for (ValueId element : node.elements) { value(element); }
                        } else if constexpr (std::is_same_v<T, Lambda>) {
                            value(node.body);
                        } else if constexpr (std::is_same_v<T, Conditional>) {
                            value(node.condition);
                            block(node.then_block);
                            value(node.otherwise);
                        } else if constexpr (std::is_same_v<T, BlockValue>) {
                            block(node.block);
                        } else if constexpr (std::is_same_v<T, Construct>) {
                            for (const Argument &argument : node.arguments) { value(argument.value); }
                        }
                        // A literal references nothing.
                    },
                    module.values[id.value].node);
            }
        };
    }  // namespace

    void collect_binding_uses(const Module &module, BlockId block, BindingUses &uses) { Collector{module, uses}.block(block); }
    void collect_binding_uses(const Module &module, StatementId statement, BindingUses &uses) {
        Collector{module, uses}.statement(statement);
    }
    void collect_binding_uses(const Module &module, ValueId value, BindingUses &uses) { Collector{module, uses}.value(value); }

    BindingUses binding_uses(const Module &module, BlockId block) {
        BindingUses uses;
        collect_binding_uses(module, block, uses);
        return uses;
    }

    BindingUses binding_uses(const Module &module, ValueId value) {
        BindingUses uses;
        collect_binding_uses(module, value, uses);
        return uses;
    }
}  // namespace hgl::hgraph_ir
