"""Compatibility shim; canonical implementation is operations.log_dq_check_result."""
import runpy as _runpy
import sys as _sys
from operations import log_dq_check_result as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.log_dq_check_result", run_name="__main__")
