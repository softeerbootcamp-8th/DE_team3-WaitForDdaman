"""Compatibility shim; canonical implementation is serving.station_risk_shared."""
import runpy as _runpy
from serving import station_risk_shared as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("serving.station_risk_shared", run_name="__main__")
