# wiring_autodoc_extension.py

import inspect
from sphinx.util.inspect import getdoc

from hgraph import WiringNodeClass, graph, TS, PreResolvedWiringNodeWrapper


def setup(app):
    """
    Setup function for the Sphinx extension.
    Connects the required events to process WiringNodeClass.
    """
    app.connect("autodoc-process-signature", replace_wiring_node_signature, 100)
    return {
        "version": "1.0",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }


def replace_wiring_node_signature(app, what, name, obj, options, signature, return_annotation):
    """
    Replaces the signature of a WiringNodeClass object with the signature of the wrapped function.

    :param app: The Sphinx application object.
    :param what: The type of the object (e.g., 'function', 'method', etc.).
    :param name: The fully qualified name of the object being documented.
    :param obj: The actual object being documented.
    :param options: The options passed to the autodoc directive.
    :param signature: The signature of the object (if any).
    :param return_annotation: The return annotation of the object (if any).
    :return: A tuple containing the updated signature and return annotation.
    """
    # Check if the object is an instance of WiringNodeClass
    if isinstance(obj, PreResolvedWiringNodeWrapper):
        obj = obj.underlying_node

    if isinstance(obj, WiringNodeClass):
        self = obj.signature
        # Extract the signature of the wrapped function
        args = (f"{arg}: {str(self.input_types[arg])}" for arg in self.args if not arg.startswith("_"))
        return_ = "" if self.output_type is None else f"{str(self.output_type)}"
        return f"({', '.join(args)})", return_

    # If the object is not a WiringNodeClass, leave it unchanged
    return signature, return_annotation


# Example usage
if __name__ == "__main__":

    def my_function(a: TS[int], b: TS[float]) -> TS[str]:
        """This is the docstring of my_function."""

    node = graph(my_function)

    # Mock Sphinx-generated locations to see results (for testing purposes)
    print("Signature:", inspect.signature(my_function))
    print("Docstring:", getdoc(my_function))

    print("Signature_node:", inspect.signature(node))
    print("Docstring_node:", getdoc(node))
