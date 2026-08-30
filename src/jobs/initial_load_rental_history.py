"""Compatibility shim; canonical implementation is bronze.initial_load_rental_history."""
import runpy as _runpy
from bronze import initial_load_rental_history as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.initial_load_rental_history", run_name="__main__")
