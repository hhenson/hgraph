#include <hgraph/types/constants.h>

namespace hgraph {
    static nb::object REMOVE;
    static nb::object REMOVE_IF_EXISTS;

    nb::object get_remove() {
        if (!REMOVE.is_valid()) {
            REMOVE = nb::module_::import_("hgraph").attr("REMOVE");
        }
        return REMOVE;
    }

    nb::object get_remove_if_exists() {
        if (!REMOVE_IF_EXISTS.is_valid()) {
            REMOVE_IF_EXISTS = nb::module_::import_("hgraph").attr("REMOVE_IF_EXISTS");
        }
        return REMOVE_IF_EXISTS;
    }

    static nb::object REMOVED;

    nb::object get_removed() {
        if (!REMOVED.is_valid()) { REMOVED = nb::module_::import_("hgraph").attr("Removed"); }
        return REMOVED;
    }

    static nb::object FROZENSET;

    nb::object get_frozenset() {
        if (!FROZENSET.is_valid()) { FROZENSET = nb::module_::import_("builtins").attr("frozenset"); }
        return FROZENSET;
    }

    static nb::object FROZENDICT;

    nb::object get_frozendict() {
        if (!FROZENDICT.is_valid()) { FROZENDICT = nb::module_::import_("frozendict").attr("frozendict"); }
        return FROZENDICT;
    }

    static nb::object KEY_SET_ID;

    nb::object get_key_set_id() {
        if (!KEY_SET_ID.is_valid()) { KEY_SET_ID = nb::module_::import_("hgraph").attr("KEY_SET_ID"); }
        return KEY_SET_ID;
    }

    static nb::object OBJECT;

    nb::object get_object() {
        if (!OBJECT.is_valid()) { OBJECT = nb::module_::import_("builtins").attr("object"); }
        return OBJECT;
    }

    static nb::object RECORD_REPLAY_CONTEXT;

    nb::object get_record_replay_context() {
        if (!RECORD_REPLAY_CONTEXT.is_valid()) {
            RECORD_REPLAY_CONTEXT = nb::module_::import_("hgraph._operators._record_replay").
                    attr("RecordReplayContext");
        }
        return RECORD_REPLAY_CONTEXT;
    }

    static nb::object RECORD_REPLAY_ENUM;

    nb::object get_record_replay_enum() {
        if (!RECORD_REPLAY_ENUM.is_valid()) {
            RECORD_REPLAY_ENUM = nb::module_::import_("hgraph._operators._record_replay").attr("RecordReplayEnum");
        }
        return RECORD_REPLAY_ENUM;
    }

    static nb::object REPLAY_CONST_FN;

    nb::object get_replay_const_fn() {
        if (!REPLAY_CONST_FN.is_valid()) {
            REPLAY_CONST_FN = nb::module_::import_("hgraph._operators._record_replay").attr("replay_const");
        }
        return REPLAY_CONST_FN;
    }

    static nb::object GET_AS_OF_FN;

    nb::object get_as_of_fn() {
        if (!GET_AS_OF_FN.is_valid()) {
            GET_AS_OF_FN = nb::module_::import_("hgraph._operators._to_table").attr("get_as_of");
        }
        return GET_AS_OF_FN;
    }

    static nb::object GET_FQ_RECORDABLE_ID_FN;

    nb::object get_fq_recordable_id_fn() {
        if (!GET_FQ_RECORDABLE_ID_FN.is_valid()) {
            GET_FQ_RECORDABLE_ID_FN = nb::module_::import_("hgraph._operators._record_replay").attr(
                "get_fq_recordable_id");
        }
        return GET_FQ_RECORDABLE_ID_FN;
    }
} // namespace hgraph