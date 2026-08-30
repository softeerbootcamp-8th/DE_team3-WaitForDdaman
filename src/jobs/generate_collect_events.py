"""Compatibility shim; canonical implementation is bronze.generate_collect_events."""
import runpy as _runpy
from bronze import generate_collect_events as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.generate_collect_events", run_name="__main__")
