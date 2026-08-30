"""Compatibility shim; canonical implementation is operations.check_bronze_gap."""
import runpy as _runpy
from operations import check_bronze_gap as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.check_bronze_gap", run_name="__main__")
