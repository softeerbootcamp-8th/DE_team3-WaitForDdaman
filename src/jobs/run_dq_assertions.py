"""Compatibility shim; canonical implementation is operations.run_dq_assertions."""
import runpy as _runpy
from operations import run_dq_assertions as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.run_dq_assertions", run_name="__main__")
