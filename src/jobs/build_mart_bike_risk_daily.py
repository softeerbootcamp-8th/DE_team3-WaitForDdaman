"""Compatibility shim; canonical implementation is serving.build_mart_bike_risk_daily."""
import runpy as _runpy
import sys as _sys
from serving import build_mart_bike_risk_daily as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("serving.build_mart_bike_risk_daily", run_name="__main__")
