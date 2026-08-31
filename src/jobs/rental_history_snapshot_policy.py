"""Compatibility shim; canonical implementation is bronze.rental_history_snapshot_policy."""
import runpy as _runpy
import sys as _sys
from bronze import rental_history_snapshot_policy as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.rental_history_snapshot_policy", run_name="__main__")
