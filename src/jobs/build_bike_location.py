"""Compatibility shim; canonical implementation is gold.build_bike_location."""
import runpy as _runpy
from gold import build_bike_location as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("gold.build_bike_location", run_name="__main__")
