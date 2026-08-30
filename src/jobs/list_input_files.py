"""Compatibility shim; canonical implementation is bronze.list_input_files."""
import runpy as _runpy
from bronze import list_input_files as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.list_input_files", run_name="__main__")
