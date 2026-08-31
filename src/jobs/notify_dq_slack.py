"""Compatibility shim; canonical implementation is operations.notify_dq_slack."""
import runpy as _runpy
import sys as _sys
from operations import notify_dq_slack as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.notify_dq_slack", run_name="__main__")
