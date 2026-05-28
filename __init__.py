"""Directory-install entry point for the Hermes RingCentral plugin."""

try:
    from .ringcentral import register
except ImportError:
    if __package__:
        raise
    from ringcentral import register

__all__ = ["register"]
