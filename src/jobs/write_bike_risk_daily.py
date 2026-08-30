"""Compatibility shim; canonical implementation is serving.write_bike_risk_daily."""
import runpy as _runpy
from serving import write_bike_risk_daily as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("serving.write_bike_risk_daily", run_name="__main__")
