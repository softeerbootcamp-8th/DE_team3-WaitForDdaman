"""Compatibility shim; canonical implementation is ml.local_run."""
import runpy as _runpy
import sys as _sys
from ml import local_run as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("ml.local_run", run_name="__main__")
