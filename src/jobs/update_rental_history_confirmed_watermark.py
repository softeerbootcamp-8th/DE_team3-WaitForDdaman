"""Compatibility shim; canonical implementation is bronze.update_rental_history_confirmed_watermark."""
import runpy as _runpy
import sys as _sys
from bronze import update_rental_history_confirmed_watermark as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.update_rental_history_confirmed_watermark", run_name="__main__")
