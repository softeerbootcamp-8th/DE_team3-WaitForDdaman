"""Compatibility shim; canonical implementation is gold.run_risk_scoring_model."""
import runpy as _runpy
from gold import run_risk_scoring_model as _canonical
globals().update({k: v for k, v in vars(_canonical).items() if k != "__name__"})

if __name__ == "__main__":
    _runpy.run_module("gold.run_risk_scoring_model", run_name="__main__")
