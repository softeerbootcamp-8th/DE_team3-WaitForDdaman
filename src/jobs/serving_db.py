"""Compatibility shim; canonical implementation is serving.serving_db."""
import runpy as _runpy
from serving import serving_db as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("serving.serving_db", run_name="__main__")
