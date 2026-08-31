"""Compatibility shim; canonical implementation is operations.advance_completion_watermark."""
import runpy as _runpy
import sys as _sys
from operations import advance_completion_watermark as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.advance_completion_watermark", run_name="__main__")
