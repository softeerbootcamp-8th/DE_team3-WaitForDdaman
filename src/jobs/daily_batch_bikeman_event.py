"""Compatibility shim; canonical implementation is bronze.daily_batch_bikeman_event."""
import runpy as _runpy
from bronze import daily_batch_bikeman_event as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.daily_batch_bikeman_event", run_name="__main__")
