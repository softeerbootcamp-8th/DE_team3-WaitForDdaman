"""Compatibility shim; canonical implementation is operations.compute_silver_rental_history_backfill_ranges."""
import runpy as _runpy
from operations import compute_silver_rental_history_backfill_ranges as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.compute_silver_rental_history_backfill_ranges", run_name="__main__")
