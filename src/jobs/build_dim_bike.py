"""Compatibility shim; canonical implementation is gold.build_dim_bike."""
import runpy as _runpy
import sys as _sys
from gold import build_dim_bike as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("gold.build_dim_bike", run_name="__main__")
