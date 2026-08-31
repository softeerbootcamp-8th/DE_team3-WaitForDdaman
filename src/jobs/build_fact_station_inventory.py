"""Compatibility shim; canonical implementation is gold.build_fact_station_inventory."""
import runpy as _runpy
import sys as _sys
from gold import build_fact_station_inventory as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("gold.build_fact_station_inventory", run_name="__main__")
