"""
This sandbox has no internet access, so `pytest` can't be pip-installed here.
This script imports every test_* function from test_pipeline_logic.py and runs
them directly with plain asserts, so we get a genuine pass/fail result now.

On your own machine (with internet), just run: pytest tests/ -v
It will execute the exact same test functions via the real pytest runner.
"""
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_pipeline_logic as t

test_fns = [getattr(t, name) for name in dir(t) if name.startswith("test_")]

passed, failed = 0, 0
for fn in test_fns:
    try:
        fn()
        print(f"PASS  {fn.__name__}")
        passed += 1
    except AssertionError as e:
        print(f"FAIL  {fn.__name__}  -> {e}")
        failed += 1
    except Exception as e:
        print(f"ERROR {fn.__name__}  -> {type(e).__name__}: {e}")
        traceback.print_exc()
        failed += 1

print(f"\n{passed} passed, {failed} failed out of {len(test_fns)} tests")
sys.exit(1 if failed else 0)
