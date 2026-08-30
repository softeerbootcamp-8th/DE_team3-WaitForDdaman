"""Compatibility shim; canonical implementation is silver.transform_silver_rental_history."""
import runpy as _runpy
from silver import transform_silver_rental_history as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("silver.transform_silver_rental_history", run_name="__main__")
