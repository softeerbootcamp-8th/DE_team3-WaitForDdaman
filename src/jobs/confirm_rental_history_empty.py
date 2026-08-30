"""Compatibility shim; canonical implementation is bronze.confirm_rental_history_empty."""
import runpy as _runpy
from bronze import confirm_rental_history_empty as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.confirm_rental_history_empty", run_name="__main__")
