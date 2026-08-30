"""Compatibility shim; canonical implementation is ml.evaluate."""
import runpy as _runpy
from ml import evaluate as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("ml.evaluate", run_name="__main__")
