"""Compatibility shim; canonical implementation is ml.samples."""
import runpy as _runpy
from ml import samples as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("ml.samples", run_name="__main__")
