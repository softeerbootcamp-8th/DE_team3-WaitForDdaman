"""Compatibility shim; canonical implementation is bronze.collect_failure_report_raw."""
import runpy as _runpy
import sys as _sys
from bronze import collect_failure_report_raw as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.collect_failure_report_raw", run_name="__main__")
