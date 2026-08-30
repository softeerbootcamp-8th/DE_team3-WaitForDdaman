"""Compatibility shim; canonical implementation is bronze.select_rental_history_snapshot."""
import runpy as _runpy
from bronze import select_rental_history_snapshot as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.select_rental_history_snapshot", run_name="__main__")
