"""Compatibility shim; canonical implementation is ml.sql_engine."""
import runpy as _runpy
import sys as _sys
from ml import sql_engine as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("ml.sql_engine", run_name="__main__")
