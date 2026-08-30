"""Compatibility shim; canonical implementation is operations.advance_silver_rental_history_watermark."""
import runpy as _runpy
from operations import advance_silver_rental_history_watermark as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.advance_silver_rental_history_watermark", run_name="__main__")
