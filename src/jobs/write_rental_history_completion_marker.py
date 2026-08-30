"""Compatibility shim; canonical implementation is bronze.write_rental_history_completion_marker."""
import runpy as _runpy
from bronze import write_rental_history_completion_marker as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.write_rental_history_completion_marker", run_name="__main__")
