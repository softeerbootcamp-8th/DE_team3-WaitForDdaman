"""Compatibility shim; canonical implementation is operations.check_watermark_date."""
import runpy as _runpy
from operations import check_watermark_date as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.check_watermark_date", run_name="__main__")
