"""Compatibility shim; canonical implementation is serving.write_station_daily."""
import runpy as _runpy
import sys as _sys
from serving import write_station_daily as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("serving.write_station_daily", run_name="__main__")
