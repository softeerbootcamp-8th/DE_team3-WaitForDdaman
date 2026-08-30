"""Compatibility shim; canonical implementation is bronze.stage_initial_load_files."""
import runpy as _runpy
from bronze import stage_initial_load_files as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.stage_initial_load_files", run_name="__main__")
