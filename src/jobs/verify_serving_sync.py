"""Compatibility shim; canonical implementation is serving.verify_serving_sync."""
import runpy as _runpy
from serving import verify_serving_sync as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("serving.verify_serving_sync", run_name="__main__")
