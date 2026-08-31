"""Compatibility shim; canonical implementation is operations.bootstrap_silver_watermark."""
import runpy as _runpy
import sys as _sys
from operations import bootstrap_silver_watermark as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.bootstrap_silver_watermark", run_name="__main__")
