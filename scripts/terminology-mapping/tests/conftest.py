"""`count_occurrences.py` is a standalone script, not a package member.

It is deliberately importable with nothing but the stdlib — the HPC node runs it
under a different interpreter and a different project's environment — so the
tests reach it by path rather than by making the occurrences directory a
package, which would be a change to the thing under test.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "occurrences"))
