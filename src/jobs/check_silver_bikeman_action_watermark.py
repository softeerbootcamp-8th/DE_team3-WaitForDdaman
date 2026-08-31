"""Compatibility shim; canonical implementation is operations.check_silver_bikeman_action_watermark."""
import runpy as _runpy
import sys as _sys
from operations import check_silver_bikeman_action_watermark as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.check_silver_bikeman_action_watermark", run_name="__main__")
