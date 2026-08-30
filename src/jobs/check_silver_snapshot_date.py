"""Compatibility shim; canonical implementation is operations.check_silver_snapshot_date."""
import runpy as _runpy
from operations import check_silver_snapshot_date as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.check_silver_snapshot_date", run_name="__main__")
