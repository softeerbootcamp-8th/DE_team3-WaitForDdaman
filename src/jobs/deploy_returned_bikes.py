"""Compatibility shim; canonical implementation is bronze.deploy_returned_bikes."""
import runpy as _runpy
import sys as _sys
from bronze import deploy_returned_bikes as _canonical
_sys.modules[__name__] = _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.deploy_returned_bikes", run_name="__main__")
