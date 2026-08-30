"""Compatibility shim; canonical implementation is bronze.bikeman_connection."""
import runpy as _runpy
from bronze import bikeman_connection as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.bikeman_connection", run_name="__main__")
