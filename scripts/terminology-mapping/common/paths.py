"""Canonical locations inside scripts/terminology-mapping/.

Everything is derived from this file's own location so the scripts work
regardless of the current working directory.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# External release files, one folder per code system then per year. Overridable
# with $ICD_SOURCE_DIR for checkouts that keep the (large) sources elsewhere.
SOURCES = Path(os.environ.get("ICD_SOURCE_DIR", ROOT / "sources"))

# Generated FHIR resources, flat, named ResourceType-id.json.
OUTPUT = ROOT / "output"

# The MIMIC IG resources every source enumeration is read from — a committed
# snapshot, pinned by sha256 in ig-resources/manifest.json, NOT a path into a
# sibling IG checkout. Half of these files are FSH-authored and gitignored in
# the IG, so reading them there made a build depend on whether `sushi .` had
# been run in another repo's working tree. See sync_ig_resources.py.
IG_RESOURCES = Path(os.environ.get("IG_RESOURCES", ROOT / "ig-resources"))

# Where `make sync-ig` refreshes the snapshot FROM. Only the sync needs it; a
# build never reads it, which is the whole point of the snapshot.
MIMIC_IG_DIR = os.environ.get("MIMIC_IG_DIR")

# sha256 of every ICD source file that influences a built CodeSystem. Committed,
# so unlike the CodeSystems themselves it can date a resource — see
# conceptmaps/lib/built.default_date.
INPUT_MANIFEST = ROOT / "input-manifest.json"

# Per-code occurrence counts extracted once on the HPC node and committed. An
# OPTIONAL input: without it build_statistics.py reports code coverage alone.
# See occurrences/README.md.
OCCURRENCES = ROOT / "occurrences"
