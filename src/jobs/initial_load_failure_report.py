"""Compatibility shim; canonical implementation is bronze.initial_load_failure_report."""
import runpy as _runpy
import sys as _sys
from bronze import initial_load_failure_report as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.initial_load_failure_report", run_name="__main__")
