"""Hermes RingCentral platform adapter plugin."""

from adapter import (
    RingCentralAdapter,
    check_requirements,
    _content_type_for_filename,
    _env_enablement,
    _is_connected,
    _standalone_send,
    DEFAULT_SERVER_URL,
    register,
)
from rc_ws import RingCentralWebSocket
import adapter as _rc_adapter_module

__all__ = [
    "register",
    "RingCentralAdapter",
    "RingCentralWebSocket",
    "check_requirements",
    "_content_type_for_filename",
    "_env_enablement",
    "_is_connected",
    "_standalone_send",
    "_rc_adapter_module",
    "DEFAULT_SERVER_URL",
]
