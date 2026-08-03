"""Tests for debugger-independent common ABI formatting."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "tools" / "debugger"))

import hgraph_debug_common as common


def node_schema(**overrides):
    snapshot = {
        "magic": common.SCHEMA_HEADER_MAGIC,
        "abi_version": common.SCHEMA_HEADER_ABI_VERSION,
        "family": 3,
        "kind": 0,
        "label": "compute[int]",
        "introspection": 0,
    }
    snapshot.update(overrides)
    return snapshot


def node_record(**overrides):
    snapshot = {
        "magic": common.TYPE_RECORD_MAGIC,
        "abi_version": common.TYPE_RECORD_ABI_VERSION,
        "role": 3,
        "reserved0": 0,
        "ops_abi_version": 1,
        "reserved1": 0,
        "capabilities": (1 << 4) | (1 << 9),
        "implementation_label": "hgraph.node.native",
        "schema": node_schema(),
        "plan": 0x1000,
        "ops": 0x2000,
        "debug": 0,
        "external_value_owner": 0,
    }
    snapshot.update(overrides)
    return snapshot


def atomic_descriptor(**overrides):
    snapshot = {
        "magic": common.DEBUG_DESCRIPTOR_MAGIC,
        "abi_version": common.DEBUG_DESCRIPTOR_ABI_VERSION,
        "layout": 1,
        "atomic_kind": 2,
        "flags": 0,
        "field_count": 0,
        "fields": 0,
        "validity_offset": 0,
        "validity_word_size": 0,
        "reserved0": 0,
        "key_type": 0,
        "element_type": 0,
        "dynamic_layout": 0,
        "time_series_layout": 0,
    }
    snapshot.update(overrides)
    return snapshot


def dynamic_layout(**overrides):
    snapshot = {
        "magic": common.DEBUG_DYNAMIC_LAYOUT_MAGIC,
        "abi_version": common.DEBUG_DYNAMIC_LAYOUT_ABI_VERSION,
        "kind": 2,
        "reserved0": 0,
        "flags": common.DEBUG_DYNAMIC_DATA_INDIRECT
        | common.DEBUG_DYNAMIC_DATA_POINTER_TABLE
        | common.DEBUG_DYNAMIC_HAS_SLOT_STATE,
        "key_auxiliary_offset": 0,
        "size_offset": 16,
        "size_constant": 0,
        "data_offset": 8,
        "stride": 4,
        "key_data_offset": 0,
        "key_stride": 0,
        "state_offset": 24,
        "auxiliary_offset": 0,
        "entry_offset": 0,
    }
    snapshot.update(overrides)
    return snapshot


class CommonDebuggerFormattingTest(unittest.TestCase):
    def test_schema_summary_uses_common_classification(self):
        self.assertTrue(common.schema_valid(node_schema()))
        self.assertEqual(
            common.schema_summary(node_schema()),
            'SchemaHeader{valid family=Node kind=Compute(0) abi=1 label="compute[int]"}',
        )

    def test_schema_validation_rejects_unknown_abi_and_family(self):
        self.assertFalse(common.schema_valid(node_schema(abi_version=2)))
        self.assertFalse(common.schema_valid(node_schema(family=9)))

    def test_record_summary_shows_semantic_and_implementation_identity(self):
        snapshot = node_record()
        self.assertTrue(common.record_valid(snapshot))
        summary = common.record_summary(snapshot)
        self.assertIn("valid Node/Runtime", summary)
        self.assertIn('semantic="compute[int]"', summary)
        self.assertIn('implementation="hgraph.node.native"', summary)
        self.assertIn("capabilities=mutable|viewable", summary)
        self.assertIn("plan=0x1000 ops=0x2000 debug=null", summary)

    def test_record_validation_rejects_role_and_required_pointer_mismatches(self):
        self.assertFalse(common.record_valid(node_record(role=1)))
        self.assertFalse(common.record_valid(node_record(plan=0)))
        self.assertFalse(common.record_valid(node_record(ops_abi_version=0)))
        self.assertFalse(common.record_valid(node_record(capabilities=1 << 20)))

    def test_pointer_summaries_cover_unbound_typed_null_live_and_malformed(self):
        record = node_record()
        self.assertEqual(common.pointer_summary("AnyPtr", 0, 0, 0), "AnyPtr{unbound}")

        typed_null = common.pointer_summary("TypedPtr", 0x3000, 0, 0, record)
        self.assertIn("TypedPtr{typed-null Node/Runtime", typed_null)
        self.assertIn("access=read-only", typed_null)

        live = common.pointer_summary("TypedPtr", 0x3000, 0x4000, 1, record)
        self.assertIn("TypedPtr{live Node/Runtime", live)
        self.assertIn("access=writable", live)
        self.assertIn("record=0x3000 data=0x4000", live)

        malformed = common.pointer_summary("AnyPtr", 0, 0x4000, 3)
        self.assertIn("AnyPtr{malformed", malformed)
        self.assertIn("access=invalid(3)", malformed)

    def test_compact_pointer_summaries_keep_only_carrier_type_and_value(self):
        record = node_record()
        self.assertEqual(
            common.compact_pointer_summary("NodePtr", 0x3000, 0x4000, 0, record),
            "NodePtr{compute[int]}",
        )
        self.assertEqual(common.typed_pointer_name(record), "NodePtr")
        self.assertEqual(
            common.compact_pointer_summary("ValuePtr", 0x3000, 0x4000, 0, {
                **record,
                "role": 1,
                "schema": {**record["schema"], "family": 1, "label": "int"},
            }, 42),
            "ValuePtr{int value=42}",
        )

    def test_family_specific_kinds_do_not_overlap_in_output(self):
        self.assertEqual(common.kind_text(1, 0), "Atomic(0)")
        self.assertEqual(common.kind_text(2, 0), "TS(0)")
        self.assertEqual(common.kind_text(3, 0), "Compute(0)")
        self.assertEqual(common.kind_text(5, 0), "Simulation(0)")
        self.assertEqual(common.kind_text(4, common.TYPE_KIND_NONE), "none")

    def test_debug_descriptor_validation_and_summary_are_data_only(self):
        descriptor = atomic_descriptor()
        self.assertTrue(common.debug_descriptor_valid(descriptor))
        summary = common.debug_descriptor_summary(descriptor)
        self.assertIn("valid layout=Atomic atomic=SignedInteger", summary)
        self.assertFalse(common.debug_descriptor_valid(atomic_descriptor(abi_version=4)))
        self.assertFalse(common.debug_descriptor_valid(atomic_descriptor(flags=4)))
        self.assertTrue(
            common.debug_descriptor_valid(
                atomic_descriptor(
                    layout=2,
                    atomic_kind=0,
                    flags=common.DEBUG_DESCRIPTOR_HAS_VALIDITY,
                    field_count=2,
                    fields=0x5000,
                    validity_offset=24,
                    validity_word_size=8,
                )
            )
        )

    def test_atomic_decoding_obeys_descriptor_kind_size_and_byte_order(self):
        self.assertEqual(common.decode_atomic(1, b"\x01", "little"), True)
        self.assertEqual(common.decode_atomic(2, b"\xfe\xff", "little"), -2)
        self.assertEqual(common.decode_atomic(3, b"\x01\x00", "big"), 256)
        self.assertAlmostEqual(common.decode_atomic(4, b"\x00\x00\xc0\x3f", "little"), 1.5)
        self.assertIs(common.decode_atomic(2, b"\x00" * 3, "little"), common._MISSING)

    def test_time_series_descriptor_requires_family_layout(self):
        descriptor = atomic_descriptor(
            layout=7,
            atomic_kind=0,
            time_series_layout=0x6000,
        )
        self.assertTrue(common.debug_descriptor_valid(descriptor))
        self.assertFalse(
            common.debug_descriptor_valid(
                atomic_descriptor(layout=7, atomic_kind=0, time_series_layout=0)
            )
        )

    def test_fixed_field_validity_uses_word_and_bit_indices(self):
        words = (1 | (1 << 63)).to_bytes(8, "little") + (1 << 1).to_bytes(8, "little")
        self.assertTrue(common.field_is_set(words, 0, 8, "little"))
        self.assertTrue(common.field_is_set(words, 63, 8, "little"))
        self.assertFalse(common.field_is_set(words, 64, 8, "little"))
        self.assertTrue(common.field_is_set(words, 65, 8, "little"))

    def test_dynamic_layout_validation_rejects_ambiguous_storage(self):
        snapshot = dynamic_layout()
        self.assertTrue(common.debug_dynamic_layout_valid(snapshot))
        self.assertIn("valid kind=StableSlots", common.debug_dynamic_layout_summary(snapshot))
        self.assertFalse(common.debug_dynamic_layout_valid(dynamic_layout(abi_version=1)))
        self.assertFalse(common.debug_dynamic_layout_valid(dynamic_layout(flags=1 << 20)))
        self.assertFalse(
            common.debug_dynamic_layout_valid(
                dynamic_layout(flags=common.DEBUG_DYNAMIC_DATA_INDIRECT)
            )
        )
        self.assertTrue(
            common.debug_dynamic_layout_valid(
                dynamic_layout(
                    flags=dynamic_layout()["flags"]
                    | common.DEBUG_DYNAMIC_ELEMENTS_ARE_POINTERS
                )
            )
        )
        self.assertFalse(
            common.debug_dynamic_layout_valid(
                dynamic_layout(
                    flags=dynamic_layout()["flags"]
                    | common.DEBUG_DYNAMIC_ELEMENTS_ARE_OWNERS
                    | common.DEBUG_DYNAMIC_ELEMENTS_ARE_POINTERS
                )
            )
        )
        tagged_flags = (
            dynamic_layout()["flags"]
            | common.DEBUG_DYNAMIC_DATA_POINTERS_TAGGED
            | common.DEBUG_DYNAMIC_SLOT_STATE_TAGGED
        )
        self.assertTrue(
            common.debug_dynamic_layout_valid(dynamic_layout(flags=tagged_flags))
        )
        erased_flags = (
            dynamic_layout()["flags"]
            | common.DEBUG_DYNAMIC_SLOT_INDEX_INDIRECT
            | common.DEBUG_DYNAMIC_SLOT_SIZE_INDIRECT
        )
        self.assertTrue(
            common.debug_dynamic_layout_valid(
                dynamic_layout(flags=erased_flags, auxiliary_offset=32)
            )
        )
        keyed_erased_flags = (
            erased_flags
            | common.DEBUG_DYNAMIC_KEY_DATA_INDIRECT
            | common.DEBUG_DYNAMIC_KEY_DATA_POINTER_TABLE
        )
        self.assertTrue(
            common.debug_dynamic_layout_valid(
                dynamic_layout(
                    flags=keyed_erased_flags,
                    key_stride=8,
                    key_auxiliary_offset=48,
                )
            )
        )
        self.assertFalse(
            common.debug_dynamic_layout_valid(
                dynamic_layout(
                    flags=dynamic_layout()["flags"]
                    | common.DEBUG_DYNAMIC_SLOT_SIZE_INDIRECT
                )
            )
        )
        self.assertFalse(
            common.debug_dynamic_layout_valid(
                dynamic_layout(
                    flags=tagged_flags & ~common.DEBUG_DYNAMIC_DATA_POINTERS_TAGGED
                )
            )
        )
        self.assertFalse(
            common.debug_dynamic_layout_valid(
                dynamic_layout(
                    flags=tagged_flags & ~common.DEBUG_DYNAMIC_HAS_SLOT_STATE
                )
            )
        )

        sequence = atomic_descriptor(
            layout=3,
            atomic_kind=0,
            element_type=0x6000,
            dynamic_layout=0x7000,
        )
        self.assertTrue(common.debug_descriptor_valid(sequence))
        self.assertFalse(common.debug_descriptor_valid({**sequence, "dynamic_layout": 0}))


if __name__ == "__main__":
    unittest.main()
