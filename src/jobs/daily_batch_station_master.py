"""Compatibility shim; canonical implementation is bronze.daily_batch_station_master."""
import runpy as _runpy
from bronze import daily_batch_station_master as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.daily_batch_station_master", run_name="__main__")
