"""
Common utility functions for autofhir
"""

DEFAULT_CONFIG = "config/ARCH.toml"


def maybe(x, func, default=None):
    return func(x) if x is not None else default
