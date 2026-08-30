"""Compatibility shim; canonical implementation is silver.silver_failure_report."""
import runpy as _runpy
from silver import silver_failure_report as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("silver.silver_failure_report", run_name="__main__")
