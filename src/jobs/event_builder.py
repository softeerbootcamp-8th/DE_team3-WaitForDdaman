"""Compatibility shim; canonical implementation is bronze.event_builder."""
import runpy as _runpy
from bronze import event_builder as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.event_builder", run_name="__main__")
