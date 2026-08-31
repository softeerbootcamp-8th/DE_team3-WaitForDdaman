"""Compatibility shim; canonical implementation is silver.silver_bikeman_action."""
import runpy as _runpy
import sys as _sys
from silver import silver_bikeman_action as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("silver.silver_bikeman_action", run_name="__main__")
