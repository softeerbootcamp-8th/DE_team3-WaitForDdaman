"""Compatibility shim; canonical implementation is gold.build_station_active."""
import runpy as _runpy
from gold import build_station_active as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("gold.build_station_active", run_name="__main__")
