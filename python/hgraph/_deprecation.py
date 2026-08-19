"""One definition of the persistence compatibility deprecation message.

RFC 0025's removal policy: the compatibility layer is retained in full
through the pre-1.0 bridge release and removed at 1.0, together with the
``hgraph.adaptors`` package itself (RFC 0005). Warning early is deliberate --
the bridge release is not imminent, so the window is long by construction,
and the cost of an early warning is a line of output where the cost of a late
one is a user who never saw it.

The release string lives HERE and nowhere else. It was previously inlined in
one message, which then said 0.9 after the policy settled on 1.0; a user
reading that message was told the wrong thing. A second copy is a second
chance to go stale.
"""

import warnings

#: The release that removes the persistence compatibility layer.
PERSISTENCE_REMOVAL_RELEASE = "1.0"


def moved_to_persistence(name: str, replacement: str) -> str:
    """The deprecation message for a name that moved to hgraph-persistence."""
    return (
        f"{name} moved to hgraph-persistence in 0.8; use {replacement} instead. "
        f"The compatibility alias is removed in {PERSISTENCE_REMOVAL_RELEASE}."
    )


def deprecated_compat_name(name: str, replacement: str) -> str:
    """The deprecation message for a 0.5 spelling that did not move packages.

    Separate from ``moved_to_persistence`` because saying a name "moved to
    hgraph-persistence" when it was merely renamed inside core sends the
    reader to the wrong distribution.
    """
    return (
        f"{name} is a 0.5 compatibility name; use {replacement} instead. "
        f"It is removed in {PERSISTENCE_REMOVAL_RELEASE}."
    )


def warn_moved_to_persistence(name: str, replacement: str, stacklevel: int = 3) -> None:
    """Emit the deprecation for a moved name.

    ``stacklevel`` defaults to 3 so the warning points at the caller of the
    shim rather than at the shim itself: 1 is this function, 2 is the shim.
    """
    warnings.warn(
        moved_to_persistence(name, replacement),
        DeprecationWarning,
        stacklevel=stacklevel,
    )


def warn_deprecated_compat_name(
    name: str, replacement: str, stacklevel: int = 3
) -> None:
    """Emit the deprecation for a retained 0.5 spelling."""
    warnings.warn(
        deprecated_compat_name(name, replacement),
        DeprecationWarning,
        stacklevel=stacklevel,
    )
