import os

__all__ = ("HG_TYPE_CHECKING",)


# This controls if we are checking types when setting values
# Typically when performance is critical we will turn this off, it is recommended to leave it on during development
HG_TYPE_CHECKING = os.environ.get("HG_TYPE_CHECKING", "1").lower() in ("1", "true", "yes", "on")
