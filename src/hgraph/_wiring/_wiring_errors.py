# Provide a set of standard error messages to help with debugging
import typing
from abc import abstractmethod, ABC

from hgraph._wiring._wiring_context import WIRING_CONTEXT

if typing.TYPE_CHECKING:
    from hgraph._types._type_meta_data import HgTypeMetaData

__all__ = ("WiringError", "ArgumentBindingErrors", "IncorrectTypeBinding", "TemplateTypeIncompatibleResolution",
           "MissingInputsError", "NoTimeSeriesInputsError", "InvalidArgumentsProvided", "CustomMessageWiringError",
           "RequirementsNotMetWiringError")


class WiringError(RuntimeError, ABC):
    """
    Wiring errors are raised during the wiring phase of the graph building process.
    These errors have the ability to print out largely useful information about the error in order to easily
    track down the source of the error.
    """

    def _print_error(self, msg):
        import sys
        print(f"\n"
              f"Wiring Error\n"
              f"============\n"
              f"\n"
              f"{msg}", file=sys.stderr)

    @abstractmethod
    def print_error(self):
        """Print the error with the argument information included"""


class WiringFailureError(WiringError):
    """
    Indicates that the wiring process failed with non-wiring specific exception which can be found in __cause__
    """
    pass


class ArgumentBindingErrors(WiringError, ABC):
    """
    A binding error related to an argument, these errors are often initiated in a lower level of the
    stack where the argument information is not visible, this marker allows the base information to be
    thrown and then re-thrown with the argument information added.
    """

    def __pre_init__(self):
        """Children should call this method in their __init__ method to capture the current argument information"""
        self.arg = WIRING_CONTEXT.current_arg
        self.kwargs = WIRING_CONTEXT.current_kwargs
        self.signature = WIRING_CONTEXT.current_signature

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        if not hasattr(self, "arg"):
            raise ValueError(
                "Children of ArgumentBindingErrors must have __pre_init__ called in their __init__ method "
                "prior to calling super")

    @property
    def input_value(self):
        inp_value = self.kwargs.get(self.arg)
        from hgraph import WiringPort
        if isinstance(inp_value, WiringPort) and inp_value.node_instance:
            inp_value = inp_value.node_instance.resolved_signature.signature
        else:
            inp_value = str(inp_value)
        return inp_value


class IncorrectTypeBinding(ArgumentBindingErrors):

    def __init__(self, expected_type: "HgTypeMetaData", actual_type: "HgTypeMetaData"):
        self.__pre_init__()
        self.expected_type = expected_type  # The type expected by the signature
        self.actual_type = actual_type  # The actual type wired in
        super().__init__(f"{self.arg}: {str(expected_type)} <- {str(actual_type)} is not type compatible")

    def print_error(self):
        inp_value = self.input_value
        msg = f"When resolving '{self.signature.signature}' \n" \
              f"Argument '{self.arg}: {self.expected_type}' <- '{self.actual_type}' from '{inp_value}'"
        self._print_error(msg)


class TemplateTypeIncompatibleResolution(ArgumentBindingErrors):

    def __init__(self,
                 template_type: "HgTypeMetaData",
                 existing_resolution: "HgTypeMetaData",
                 new_resolution: "HgTypeMetaData"):
        self.__pre_init__()
        self.template_type = template_type  # The type expected by the signature
        self.existing_resolution = existing_resolution  # The currently resolved type
        self.new_resolution = new_resolution  # The new resolved type
        super().__init__(f"TypeVar '{str(template_type)}' has already been resolved to "
                         f"'{str(existing_resolution)}' which does not match the type '{self.arg}: {new_resolution}'")

    def print_error(self):
        msg = f"When resolving '{self.signature.signature}' \n" \
              f"Template: '{self.template_type}' <- '{self.existing_resolution}'\n" \
              f"Argument '{self.arg}: {self.signature.input_types[self.arg]}' " \
              f" <- '{self.input_value}'\n" \
              f"Redefines template to '{self.new_resolution}'"
        self._print_error(msg)


class MissingInputsError(WiringError):

    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.signature = WIRING_CONTEXT.current_signature
        self.missing_inputs = {k: self.signature.input_types[k] for k in self.signature.args if k not in self.kwargs}
        super().__init__(f"Missing inputs: {self.missing_inputs}")

    def print_error(self):
        from hgraph import WiringPort
        provided_inputs = {k: str(v.output_type) if isinstance(v, WiringPort) else str(v) for k, v in self.kwargs.items()}
        msg = f"When resolving '{self.signature.signature}' \n" \
              f"Missing Inputs: {self.missing_inputs}\n" \
              f"Provided Inputs: {provided_inputs}"
        self._print_error(msg)


class NoTimeSeriesInputsError(WiringError):

    def __init__(self):
        self.signature = WIRING_CONTEXT.current_signature
        super().__init__("No time-series inputs provided")

    def print_error(self):
        msg = f"When resolving '{self.signature.signature}' \n" \
              "No time-series inputs provided"
        self._print_error(msg)


class InvalidArgumentsProvided(WiringError):

    def __init__(self, bad_arguments: typing.Sequence[str]):
        self.bad_arguments = tuple(bad_arguments)
        self.signature = WIRING_CONTEXT.current_signature
        super().__init__(f"Invalid arguments: {self.bad_arguments}")

    def print_error(self):
        msg = f"When resolving '{self.signature.signature}' \n" \
              f"Invalid inputs provided: {self.bad_arguments}"
        self._print_error(msg)


class CustomMessageWiringError(WiringError):
    def __init__(self, message: str):
        self.message = message
        self.signature = WIRING_CONTEXT.current_signature
        super().__init__(self.message)

    def print_error(self):
        signature = self.signature.signature if self.signature else "unnamed graph"
        msg = f"When resolving '{signature}' \n{self.message}"
        self._print_error(msg)


class RequirementsNotMetWiringError(WiringError):
    def __init__(self, message: str):
        self.message = message
        self.signature = WIRING_CONTEXT.current_signature
        super().__init__(self.message)

    def print_error(self):
        signature = self.signature.signature if self.signature else "unnamed graph"
        msg = f"Requitements not met for '{signature}' \n{self.message}"
        self._print_error(msg)

