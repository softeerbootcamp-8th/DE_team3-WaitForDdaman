"""Compatibility shim; canonical implementation is bronze.catchup_failure_report_date."""
import runpy as _runpy
from bronze import catchup_failure_report_date as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("bronze.catchup_failure_report_date", run_name="__main__")
