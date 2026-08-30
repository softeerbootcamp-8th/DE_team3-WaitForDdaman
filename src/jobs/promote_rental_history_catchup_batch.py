"""Compatibility shim; canonical implementation is bronze.promote_rental_history_catchup_batch."""
import runpy as _runpy
from bronze import promote_rental_history_catchup_batch as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.promote_rental_history_catchup_batch", run_name="__main__")
