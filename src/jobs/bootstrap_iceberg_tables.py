"""Compatibility shim; canonical implementation is operations.bootstrap_iceberg_tables."""
import runpy as _runpy
from operations import bootstrap_iceberg_tables as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("operations.bootstrap_iceberg_tables", run_name="__main__")
