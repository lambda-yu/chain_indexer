from __future__ import annotations


class AbiError(Exception):
    """Base for all AbiRegistry / decoder errors."""


class AbiNotFound(AbiError):
    """No ABI is registered under the given id."""


class DecodeFailed(AbiError):
    """An ABI was found but decoding the input failed (malformed log/calldata)."""
