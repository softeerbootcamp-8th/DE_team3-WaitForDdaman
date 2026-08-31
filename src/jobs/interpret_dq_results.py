"""Compatibility shim; canonical implementation is operations.interpret_dq_results."""
import runpy as _runpy
import sys as _sys
from operations import interpret_dq_results as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.interpret_dq_results", run_name="__main__")
