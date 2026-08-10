"""Python-facing presentation of native operator type patterns."""

from __future__ import annotations

import re


_VARIABLE = re.compile(r"~([A-Za-z_][A-Za-z0-9_]*)")


def _split_arguments(value: str) -> list[str]:
    arguments = []
    start = 0
    depth = 0
    for index, character in enumerate(value):
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
        elif character == "," and depth == 0:
            arguments.append(value[start:index].strip())
            start = index + 1
    arguments.append(value[start:].strip())
    return arguments


def _split_field(value: str) -> tuple[str | None, str]:
    depth = 0
    for index, character in enumerate(value):
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
        elif character == ":" and depth == 0:
            return value[:index].strip(), value[index + 1:].strip()
    return None, value


class PublicTypePatternFormatter:
    """Translate registry variables into the public Python generic vocabulary."""

    _BASE_NAME = {
        "scalar": "SCALAR",
        "schema": "TS_SCHEMA",
        "size": "SIZE",
        "time_series": "TIME_SERIES_TYPE",
    }

    def __init__(self):
        self._variables = {category: {} for category in self._BASE_NAME}

    @staticmethod
    def _preserved_name(name: str) -> str | None:
        upper = name.strip("_").upper()
        if upper in {"O", "OUT"}:
            return "OUT"
        match = re.fullmatch(r"([KV])(\d*)", upper)
        if match:
            return match.group(1) + (f"_{match.group(2)}" if match.group(2) else "")
        if upper in {
            "COMPOUND_SCALAR", "KEYABLE_SCALAR", "NUMBER", "SCALAR",
            "SIZE", "TS_SCHEMA", "WINDOW_SIZE", "WINDOW_SIZE_MIN",
        }:
            return upper
        return None

    def _variable(self, name: str, category: str, *, output: bool = False) -> str:
        if preserved := self._preserved_name(name):
            return preserved
        variables = self._variables[category]
        if name not in variables:
            if output and category == "time_series":
                variables[name] = "OUT"
            else:
                base = self._BASE_NAME[category]
                index = len(variables)
                variables[name] = base if index == 0 else f"{base}_{index}"
        return variables[name]

    def format(self, pattern: str, *, category: str, output: bool = False) -> str:
        """Format one scalar, time-series, schema, or size pattern."""
        pattern = pattern.strip()
        if match := _VARIABLE.fullmatch(pattern):
            return self._variable(match.group(1), category, output=output)
        if category == "size" and pattern == "0":
            return "SIZE"

        bracket = pattern.find("[")
        if bracket < 0 or not pattern.endswith("]"):
            return pattern
        head = pattern[:bracket]
        arguments = _split_arguments(pattern[bracket + 1:-1])

        if head in {"TS", "TSS"}:
            categories = ["scalar"]
        elif head == "TSL":
            categories = ["time_series", "size"]
        elif head == "TSD":
            categories = ["scalar", "time_series"]
        elif head == "TSW":
            categories = ["scalar", *(["size"] * (len(arguments) - 1))]
        elif head == "REF":
            categories = ["time_series"]
        elif head.startswith("TSB"):
            categories = ["time_series"] * len(arguments)
            if len(arguments) == 1 and _split_field(arguments[0])[0] is None:
                categories = ["schema"]
        elif head in {"Array", "array"}:
            categories = ["scalar", *(["size"] * (len(arguments) - 1))]
        else:
            categories = ["scalar"] * len(arguments)

        rendered = []
        for argument, argument_category in zip(arguments, categories):
            field_name, field_pattern = _split_field(argument)
            value = self.format(field_pattern, category=argument_category)
            rendered.append(f"{field_name}: {value}" if field_name else value)
        return f"{head}[{', '.join(rendered)}]"
