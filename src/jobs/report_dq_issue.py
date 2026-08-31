"""Compatibility shim; canonical implementation is operations.report_dq_issue."""
import runpy as _runpy
import sys as _sys
from operations import report_dq_issue as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.report_dq_issue", run_name="__main__")
