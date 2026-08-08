/**
 * Port bindings: the Port handle, port argument tags (passive/pass_through/
 * no_key), the TSB/TSL structural packing surface (tsb_port/tsl_port/
 * bundle_port - Howard's REF ruling lives on bundle_port), and
 * TimeSeriesRef.
 */
#include "py_bindings.h"
#include "py_wiring.h"

namespace nb = nanobind;
using namespace hgraph;
using namespace hgraph::python_bridge;

namespace hgraph::python_bridge {
void bind_ports(nb::module_ &m) {
  nb::class_<PyPort>(m, "Port")
      .def_prop_ro("ts_type",
                   [](const PyPort &self) { return PyTsType{self.ref.schema}; })
      .def_prop_ro(
          "is_structural",
          [](const PyPort &self) { return self.ref.is_structural_source(); })
      .def_prop_ro(
          "node_type_info",
          [](const PyPort &self) -> nb::object {
            if (!self.ref.is_peered_source()) {
              return nb::none();
            }
            const NodeTypeRef type = self.ref.peered_node()->builder.type();
            const TypeRecord *record = type.record();
            nb::dict info;
            info["family"] =
                static_cast<std::uint8_t>(record->classification().family);
            info["role"] = static_cast<std::uint8_t>(record->role);
            info["kind"] = record->classification().kind;
            info["semantic_label"] = std::string{record->semantic_name()};
            info["implementation_label"] =
                std::string{record->implementation_name()};
            info["ops_abi_version"] = record->ops_abi_version;
            return info;
          })
      // True for a CHILD projection of a node output (a non-empty peered
      // path). Compiled nested graphs retain this path in their output
      // binding rather than materializing a copy node.
      .def_prop_ro("has_path",
                   [](const PyPort &self) {
                     return !self.ref.peered_path_or_empty().empty();
                   })
      .def_prop_ro("dereferenced", [](const PyPort &self) {
        // The descriptive-schema patch (the Port::as / reference-service
        // pattern): present a REF output as its value schema - input
        // binding inserts the REF adaptation. Applied RECURSIVELY so a
        // TSB with REF fields records its fields dereferenced
        // (eval_node parity: REF outputs record dereferenced values).
        if (self.ref.schema == nullptr) {
          return self;
        }
        if (self.ref.schema->kind == TSTypeKind::REF) {
          PyPort patched = self;
          patched.ref.schema = self.ref.schema->referenced_ts();
          return patched;
        }
        if (self.ref.schema->kind == TSTypeKind::TSD) {
          // DEEP dereference: REF elements at ANY nesting level patch
          // to their referenced shape (the elementwise from-REF
          // alternative recurses through nested dictionaries).
          const std::function<const TSValueTypeMetaData *(
              const TSValueTypeMetaData *)>
              deep = [&](const TSValueTypeMetaData *schema)
              -> const TSValueTypeMetaData * {
            auto &registry = TypeRegistry::instance();
            const auto *current = registry.dereference(schema);
            if (current != nullptr && current->kind == TSTypeKind::TSD) {
              const auto *element = deep(current->element_ts());
              if (element != current->element_ts()) {
                return registry.tsd(current->key_type(), element);
              }
            }
            return current;
          };
          const auto *patched_schema = deep(self.ref.schema);
          if (patched_schema != self.ref.schema) {
            PyPort patched = self;
            patched.ref.schema = patched_schema;
            return patched; // the from-REF dict alternative resolves it
          }
        }
        if (self.ref.schema->kind == TSTypeKind::TSB) {
          const auto *fields = self.ref.schema->fields();
          const auto count = self.ref.schema->field_count();
          bool has_ref = false;
          for (std::size_t index = 0; index < count; ++index) {
            const auto *type = fields[index].type;
            if (type != nullptr && type->kind == TSTypeKind::REF) {
              has_ref = true;
              break;
            }
          }
          if (has_ref) {
            auto &registry = TypeRegistry::instance();
            std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
                patched_fields;
            patched_fields.reserve(count);
            for (std::size_t index = 0; index < count; ++index) {
              const auto *type = fields[index].type;
              while (type != nullptr && type->kind == TSTypeKind::REF) {
                type = type->referenced_ts();
              }
              patched_fields.emplace_back(std::string{fields[index].name},
                                          type);
            }
            PyPort patched = self;
            patched.ref.schema = registry.un_named_tsb(patched_fields);
            return patched;
          }
        }
        return self;
      });

  // The passive marker: a tagged COPY of the port (passivity applies to
  // the tagged usage only - Python's passive(ts)).
  m.def("passive", [](const PyPort &port) {
    return PyPort{port.ref.with_arg_tag(WiringPortRef::ArgTag::Passive)};
  });
  // map_'s multiplex markers (hgraph's pass_through/no_key wrappers).
  m.def("pass_through_tag", [](const PyPort &port) {
    return PyPort{port.ref.with_arg_tag(WiringPortRef::ArgTag::PassThrough)};
  });
  m.def("no_key_tag", [](const PyPort &port) {
    return PyPort{port.ref.with_arg_tag(WiringPortRef::ArgTag::NoKey)};
  });

  // Pack argument ports into a STRUCTURAL un-named TSB (the dict/list
  // passing model for python user nodes - any arity, one operator).
  m.def("ts_field_types", [](PyTsType ts_type) {
    nb::list result;
    if (ts_type.meta == nullptr || ts_type.meta->kind != TSTypeKind::TSB) {
      return result;
    }
    for (std::size_t index = 0; index < ts_type.meta->field_count(); ++index) {
      const auto &field = ts_type.meta->fields()[index];
      result.append(
          nb::make_tuple(std::string{field.name != nullptr ? field.name : ""},
                         PyTsType{field.type}));
    }
    return result;
  });

  m.def("tsb_port", [](PyTsType ts_type, nb::dict ports) {
    if (ts_type.meta == nullptr || ts_type.meta->kind != TSTypeKind::TSB) {
      throw nb::value_error("TSB.from_ts requires a TSB type");
    }
    if (nb::len(ports) != ts_type.meta->field_count()) {
      throw nb::value_error("TSB.from_ts requires every field exactly once");
    }
    std::vector<WiringPortRef> children;
    children.reserve(ts_type.meta->field_count());
    for (std::size_t index = 0; index < ts_type.meta->field_count(); ++index) {
      const auto &field = ts_type.meta->fields()[index];
      nb::object port = ports.attr("get")(field.name);
      if (port.is_none()) {
        throw nb::value_error(
            ("TSB.from_ts is missing field '" + std::string{field.name} + "'")
                .c_str());
      }
      children.push_back(nb::cast<PyPort &>(port).ref);
    }
    return PyPort{
        WiringPortRef::structural_source(ts_type.meta, std::move(children))};
  });

  m.def("structural_has_ref_children", [](const PyPort &port) {
    return port.ref.structural_has_reference_children();
  });

  m.def("tsb_has_ref_fields", [](PyTsType ts_type) {
    if (ts_type.meta == nullptr || ts_type.meta->kind != TSTypeKind::TSB) {
      return false;
    }
    for (std::size_t index = 0; index < ts_type.meta->field_count(); ++index) {
      if (ts_type.meta->fields()[index].type != nullptr &&
          ts_type.meta->fields()[index].type->kind == TSTypeKind::REF) {
        return true;
      }
    }
    return false;
  });

  m.def("tsb_field_names", [](PyTsType ts_type) {
    nb::list names;
    if (ts_type.meta == nullptr || ts_type.meta->kind != TSTypeKind::TSB) {
      return names;
    }
    for (std::size_t index = 0; index < ts_type.meta->field_count(); ++index) {
      names.append(
          nb::str(std::string{ts_type.meta->fields()[index].name}.c_str()));
    }
    return names;
  });

  m.def("ref_port", [](PyWiring &wiring, const PyPort &port) {
    // Materialize a STRUCTURAL port as a REFERENCE output (the
    // structural-REF node): the child output becomes REF<S> without
    // copying - hgraph's combine-of-references shape.
    auto &registry = TypeRegistry::instance();
    const auto *target = registry.dereference(port.ref.schema);
    const auto *ref_schema = registry.ref(target);
    return PyPort{graph_wiring_detail::adapt_source_for_input(
        *wiring.raw, ref_schema, port.ref)};
  });

  m.def("un_named_tsb_type", [](nb::list fields) {
    std::vector<std::pair<std::string, const TSValueTypeMetaData *>>
        field_metas;
    field_metas.reserve(nb::len(fields));
    for (nb::handle item : fields) {
      auto pair = nb::cast<nb::tuple>(item);
      field_metas.emplace_back(nb::cast<std::string>(pair[0]),
                               nb::cast<PyTsType &>(pair[1]).meta);
    }
    return PyTsType{TypeRegistry::instance().un_named_tsb(field_metas)};
  });

  m.def("bundle_vt", [](const std::string &name, nb::list fields) {
    std::vector<std::pair<std::string, const ValueTypeMetaData *>> field_metas;
    field_metas.reserve(nb::len(fields));
    for (nb::handle item : fields) {
      auto pair = nb::cast<nb::tuple>(item);
      field_metas.emplace_back(nb::cast<std::string>(pair[0]),
                               nb::cast<PyValueType &>(pair[1]).meta);
    }
    return PyValueType{TypeRegistry::instance().bundle(name, field_metas)};
  });

  m.def(
      "qualified_bundle_vt",
      [](const std::string &bundle_namespace, const std::string &local_name,
         nb::list fields, nb::list parents, bool is_abstract,
         const std::string &discriminator, nb::list generic_arguments) {
        std::vector<std::pair<std::string, const ValueTypeMetaData *>>
            field_metas;
        field_metas.reserve(nb::len(fields));
        for (nb::handle item : fields) {
          auto pair = nb::cast<nb::tuple>(item);
          field_metas.emplace_back(nb::cast<std::string>(pair[0]),
                                   nb::cast<PyValueType &>(pair[1]).meta);
        }
        std::vector<const ValueTypeMetaData *> parent_metas;
        parent_metas.reserve(nb::len(parents));
        for (nb::handle parent : parents) {
          parent_metas.push_back(nb::cast<PyValueType &>(parent).meta);
        }
        std::vector<const ValueTypeMetaData *> generic_metas;
        generic_metas.reserve(nb::len(generic_arguments));
        for (nb::handle argument : generic_arguments) {
          generic_metas.push_back(nb::cast<PyValueType &>(argument).meta);
        }
        return PyValueType{TypeRegistry::instance().bundle(
            bundle_namespace, local_name, field_metas, parent_metas,
            is_abstract, discriminator, generic_metas)};
      },
      nb::arg("namespace"), nb::arg("local_name"), nb::arg("fields"),
      nb::arg("parents") = nb::list(), nb::arg("abstract") = false,
      nb::arg("discriminator") = "__type__",
      nb::arg("generic_arguments") = nb::list());

  m.def(
      "recursive_bundle_vt",
      [](const std::string &bundle_namespace, const std::string &local_name,
         nb::list fields, nb::list parents, bool is_abstract,
         const std::string &discriminator, nb::list generic_arguments) {
        std::vector<std::pair<std::string, const ValueTypeMetaData *>>
            field_metas;
        field_metas.reserve(nb::len(fields));
        for (nb::handle item : fields) {
          auto pair = nb::cast<nb::tuple>(item);
          const auto field_name = nb::cast<std::string>(pair[0]);
          field_metas.emplace_back(field_name,
                                   pair[1].is_none()
                                       ? nullptr
                                       : nb::cast<PyValueType &>(pair[1]).meta);
        }
        std::vector<const ValueTypeMetaData *> parent_metas;
        parent_metas.reserve(nb::len(parents));
        for (nb::handle parent : parents) {
          parent_metas.push_back(nb::cast<PyValueType &>(parent).meta);
        }
        std::vector<const ValueTypeMetaData *> generic_metas;
        generic_metas.reserve(nb::len(generic_arguments));
        for (nb::handle argument : generic_arguments) {
          generic_metas.push_back(nb::cast<PyValueType &>(argument).meta);
        }
        return PyValueType{TypeRegistry::instance().recursive_bundle(
            bundle_namespace, local_name, field_metas, parent_metas,
            is_abstract, discriminator, generic_metas)};
      },
      nb::arg("namespace"), nb::arg("local_name"), nb::arg("fields"),
      nb::arg("parents") = nb::list(), nb::arg("abstract") = false,
      nb::arg("discriminator") = "__type__",
      nb::arg("generic_arguments") = nb::list());

  m.def(
      "recursive_bundles_vt",
      [](nb::list definitions) {
        std::vector<RecursiveBundleDefinition> native;
        native.reserve(nb::len(definitions));
        for (nb::handle item : definitions) {
          nb::tuple definition = nb::cast<nb::tuple>(item);
          RecursiveBundleDefinition entry;
          entry.bundle_namespace = nb::cast<std::string>(definition[0]);
          entry.local_name = nb::cast<std::string>(definition[1]);
          for (nb::handle field_item : nb::cast<nb::list>(definition[2])) {
            nb::tuple field = nb::cast<nb::tuple>(field_item);
            RecursiveBundleFieldDefinition native_field;
            native_field.name = nb::cast<std::string>(field[0]);
            if (nb::isinstance<PyValueType>(field[1])) {
              native_field.type = nb::cast<PyValueType &>(field[1]).meta;
            } else {
              native_field.owned_target = nb::cast<std::size_t>(field[1]);
            }
            entry.fields.push_back(std::move(native_field));
          }
          for (nb::handle parent : nb::cast<nb::list>(definition[3])) {
            entry.parents.push_back(nb::cast<PyValueType &>(parent).meta);
          }
          entry.is_abstract = nb::cast<bool>(definition[4]);
          entry.discriminator = nb::cast<std::string>(definition[5]);
          for (nb::handle argument : nb::cast<nb::list>(definition[6])) {
            entry.generic_arguments.push_back(
                nb::cast<PyValueType &>(argument).meta);
          }
          native.push_back(std::move(entry));
        }
        nb::list result;
        for (const ValueTypeMetaData *meta :
             TypeRegistry::instance().recursive_bundles(native)) {
          result.append(nb::cast(PyValueType{meta}));
        }
        return result;
      },
      nb::arg("definitions"));

  m.def("register_bundle_class", [](const std::string &name, nb::object cls) {
    bundle_class_registry()[nb::str(name.c_str())] = std::move(cls);
  });
  m.def(
      "register_bundle_class",
      [](PyValueType type, nb::object cls, nb::object specialization,
         nb::tuple constructor_fields, bool constructor_accepts_kwargs,
         nb::tuple raw_descriptor_fields,
         nb::tuple defaulted_constructor_fields, bool force_constructor) {
        auto &info = bundle_class_info_registry()[type.meta];
        if (info.type.is_valid() && !info.type.is(cls)) {
          throw nb::type_error("Bundle schema is already registered to a "
                               "different Python class");
        }
        const bool has_specialization =
            specialization.is_valid() && !specialization.is_none();
        if (info.specialization.is_valid() != has_specialization ||
            (has_specialization &&
             PyObject_RichCompareBool(info.specialization.ptr(),
                                      specialization.ptr(), Py_EQ) != 1)) {
          if (info.type.is_valid()) {
            if (PyErr_Occurred() != nullptr) {
              nb::raise_python_error();
            }
            throw nb::type_error(
                "Bundle schema is already registered with a different "
                "Python specialization");
          }
        }
        info.type = cls;
        info.specialization =
            has_specialization ? specialization : nb::object{};
        info.allocator =
            reinterpret_cast<PyBundleClassInfo::Allocator>(PyType_GetSlot(
                reinterpret_cast<PyTypeObject *>(cls.ptr()), Py_tp_alloc));
        if (info.allocator == nullptr) {
          throw nb::type_error("CompoundScalar class has no allocation slot");
        }
        info.field_names.clear();
        info.field_names.reserve(type.meta->field_count);
        info.field_overrides.clear();
        info.field_overrides.reserve(type.meta->field_count);
        info.constructor_fields.assign(type.meta->field_count,
                                       constructor_accepts_kwargs);
        info.defaulted_constructor_fields.assign(type.meta->field_count, false);
        info.requires_constructor = force_constructor;
        std::vector<bool> raw_descriptors(type.meta->field_count, false);
        for (nb::handle constructor_field : constructor_fields) {
          const std::string name = nb::cast<std::string>(constructor_field);
          for (std::size_t index = 0; index < type.meta->field_count; ++index) {
            const char *field_name = type.meta->fields[index].name;
            if (field_name != nullptr && name == field_name) {
              info.constructor_fields[index] = true;
              break;
            }
          }
        }
        for (nb::handle raw_descriptor_field : raw_descriptor_fields) {
          const std::string name = nb::cast<std::string>(raw_descriptor_field);
          for (std::size_t index = 0; index < type.meta->field_count; ++index) {
            const char *field_name = type.meta->fields[index].name;
            if (field_name != nullptr && name == field_name) {
              raw_descriptors[index] = true;
              break;
            }
          }
        }
        for (nb::handle defaulted_field : defaulted_constructor_fields) {
          const std::string name = nb::cast<std::string>(defaulted_field);
          for (std::size_t index = 0; index < type.meta->field_count; ++index) {
            const char *field_name = type.meta->fields[index].name;
            if (field_name != nullptr && name == field_name) {
              info.defaulted_constructor_fields[index] = true;
              break;
            }
          }
        }
        for (std::size_t index = 0; index < type.meta->field_count; ++index) {
          const auto &field = type.meta->fields[index];
          info.field_names.emplace_back(field.name != nullptr ? field.name
                                                              : "");
          info.field_overrides.emplace_back();
          PyObject *attribute =
              PyObject_GetAttr(cls.ptr(), info.field_names.back().ptr());
          if (attribute == nullptr) {
            if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
              PyErr_Clear();
            } else {
              nb::raise_python_error();
            }
            continue;
          }
          nb::object owned_attribute = nb::steal(attribute);
          const int has_set =
              PyObject_HasAttrString(owned_attribute.ptr(), "__set__");
          if (has_set < 0) {
            nb::raise_python_error();
          }
          if (has_set == 0) {
            continue;
          }

          PyObject *override =
              PyObject_GetAttrString(owned_attribute.ptr(), "__override__");
          if (override == nullptr) {
            if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
              PyErr_Clear();
            } else {
              nb::raise_python_error();
            }
            if (!raw_descriptors[index]) {
              info.requires_constructor = true;
            }
            continue;
          }
          nb::object owned_override = nb::steal(override);
          if (PyCallable_Check(owned_override.ptr()) == 0) {
            if (!raw_descriptors[index]) {
              info.requires_constructor = true;
            }
            continue;
          }
          info.field_overrides.back() = std::move(owned_override);
        }
        bundle_class_registry()[nb::int_(
            reinterpret_cast<std::uintptr_t>(type.meta))] = std::move(cls);
      },
      nb::arg("type"), nb::arg("cls"), nb::arg("specialization") = nb::none(),
      nb::arg("constructor_fields") = nb::tuple(),
      nb::arg("constructor_accepts_kwargs") = false,
      nb::arg("raw_descriptor_fields") = nb::tuple(),
      nb::arg("defaulted_constructor_fields") = nb::tuple(),
      nb::arg("force_constructor") = false);
  m.def(
      "register_python_bundle_binding",
      [](PyValueType type) { register_python_bundle_binding(type.meta); },
      nb::arg("type"));
  m.def("register_tsb_compound_class", [](PyTsType tsb, PyValueType value) {
    if (tsb.meta == nullptr || tsb.meta->kind != TSTypeKind::TSB ||
        value.meta == nullptr ||
        value.meta->value_kind() != ValueTypeKind::Bundle) {
      throw nb::type_error(
          "register_tsb_compound_class requires TSB and Bundle schemas");
    }
    tsb_compound_value_registry()[tsb.meta] = value.meta;
  });

  m.def("tsl_element", [](const PyPort &port, std::size_t index) {
    // Fixed-TSL scalar indexing: the structural element projection
    // (zero-copy; no node) - the intended mechanism for tsl[i].
    const auto *element_schema =
        port.ref.schema != nullptr ? port.ref.schema->element_ts() : nullptr;
    return PyPort{subgraph_wiring_detail::tsl_element_ref(port.ref, index,
                                                          element_schema)};
  });

  m.def(
      "tsl_port",
      [](nb::list ports, std::optional<PyTsType> output_type) {
        if (nb::len(ports) == 0) {
          throw nb::value_error("tsl_port requires at least one port");
        }
        std::vector<WiringPortRef> children;
        children.reserve(nb::len(ports));
        const TSValueTypeMetaData *schema =
            output_type.has_value() ? output_type->meta : nullptr;
        if (schema != nullptr && schema->kind != TSTypeKind::TSL) {
          throw nb::value_error("TSL.from_ts output type must be a TSL");
        }
        if (schema != nullptr && schema->fixed_size() != 0 &&
            schema->fixed_size() != nb::len(ports)) {
          throw nb::value_error(
              "TSL.from_ts port count does not match the fixed output size");
        }
        const TSValueTypeMetaData *element =
            schema != nullptr ? schema->element_ts() : nullptr;
        for (nb::handle port : ports) {
          const WiringPortRef &ref = nb::cast<PyPort &>(port).ref;
          if (element == nullptr) {
            element = ref.schema;
          } else if (schema == nullptr && element != ref.schema) {
            auto &registry = TypeRegistry::instance();
            const auto *left = registry.dereference(element);
            const auto *right = registry.dereference(ref.schema);
            if (time_series_schema_equivalent(left, right)) {
              // A structural list of REF[T] and T children represents
              // T. Each REF child still retains its source binding, but
              // indexing and downstream consumers see one element type.
              element = left;
            } else if (graph_wiring_detail::input_accepts_output_schema(
                           element, ref.schema)) {
              // The current element is already the wider accepted type.
            } else if (graph_wiring_detail::input_accepts_output_schema(
                           ref.schema, element)) {
              element = ref.schema;
            } else {
              throw nb::value_error(
                  "TSL.from_ts requires ports of one element type");
            }
          } else if (schema != nullptr &&
                     !graph_wiring_detail::input_accepts_output_schema(
                         element, ref.schema)) {
            throw nb::value_error(
                "TSL.from_ts requires ports of one element type");
          }
          children.push_back(ref);
        }
        if (schema == nullptr) {
          schema = TypeRegistry::instance().tsl(element, children.size());
        }
        return PyPort{
            WiringPortRef::structural_source(schema, std::move(children))};
      },
      nb::arg("ports"), nb::arg("output_type") = nb::none());

  m.def("bundle_port", [](nb::list ports, nb::list reference_shapes) {
    if (nb::len(ports) == 0) {
      throw nb::value_error("bundle_port requires at least one port");
    }
    std::vector<std::pair<std::string, const TSValueTypeMetaData *>> fields;
    std::vector<WiringPortRef> children;
    fields.reserve(nb::len(ports));
    children.reserve(nb::len(ports));
    std::size_t index = 0;
    auto &registry = TypeRegistry::instance();
    for (nb::handle port : ports) {
      const WiringPortRef &ref = nb::cast<PyPort &>(port).ref;
      // Howard's REF ruling: a non-REF parameter bound to a REF source
      // accepts the DEREFERENCED value (binding inserts the from-REF
      // adaptation); a REF parameter receives the reference itself as
      // an opaque value.
      const bool has_shape = index < nb::len(reference_shapes) &&
                             nb::isinstance<PyTsType>(reference_shapes[index]);
      const bool as_ref = !has_shape && index < nb::len(reference_shapes) &&
                          nb::cast<bool>(reference_shapes[index]);
      const auto *field_schema =
          has_shape ? nb::cast<PyTsType &>(reference_shapes[index]).meta
          : as_ref
              ? (ref.schema != nullptr && ref.schema->kind == TSTypeKind::REF
                     ? ref.schema
                     : registry.ref(registry.dereference(ref.schema)))
              : registry.dereference(ref.schema);
      fields.emplace_back("_" + std::to_string(index++), field_schema);
      children.push_back(ref);
    }
    const auto *schema = registry.un_named_tsb(fields);
    return PyPort{
        WiringPortRef::structural_source(schema, std::move(children))};
  });

  nb::class_<PyOpaqueRef>(m, "TimeSeriesRef")
      .def_prop_ro("is_empty",
                   [](const PyOpaqueRef &self) {
                     return self.value.view()
                         .checked_as<TimeSeriesReference>()
                         .is_empty();
                   })
      .def_prop_ro("has_output",
                   [](const PyOpaqueRef &self) {
                     return self.value.view()
                         .checked_as<TimeSeriesReference>()
                         .has_output();
                   })
      .def_prop_ro(
          "is_valid",
          [](const PyOpaqueRef &self) {
            return self.evaluation_time != MIN_DT &&
                   self.value.view().checked_as<TimeSeriesReference>().is_valid(
                       self.evaluation_time);
          })
      .def("__eq__",
           [](const PyOpaqueRef &self, nb::handle other) {
             return nb::isinstance<PyOpaqueRef>(other) &&
                    nb::cast<PyOpaqueRef &>(other).value.view().equals(
                        self.value.view());
           })
      .def("__repr__", [](const PyOpaqueRef &self) {
        return std::string{
            self.value.view().checked_as<TimeSeriesReference>().is_empty()
                ? "<ref: empty>"
                : "<ref>"};
      });
  m.def("empty_time_series_reference", [] {
    // The registry's interned reference meta - taken from a REF schema
    // directly (every REF plan shares it; a Value built any other way
    // would fail the binding-identity checks).
    auto &registry = TypeRegistry::instance();
    const auto *ref_meta =
        registry.ref(registry.ts(scalar_descriptor<Int>::value_meta()));
    const auto type =
        ValuePlanFactory::instance().type_for(ref_meta->value_schema);
    if (!type) {
      throw std::logic_error("TimeSeriesReference meta has no canonical type");
    }
    return PyOpaqueRef{Value{type}, MIN_DT};
  });
}
} // namespace hgraph::python_bridge
