"""The built standard CodeSystems, and resolving a code into a release.

Answers come from the CodeSystems in output/ that stage 1 wrote and uploaded, so
the files and the server hold the same content. Reading them locally costs
nothing, which is what keeps the builders offline and instant.

Each code is mapped into the EARLIEST release containing it, because
`group.targetVersion` is the only place R4 lets a target release be recorded —
there is no version on group.element.target.
"""

import datetime
import json
import subprocess
import sys

from .canonical import MIMIC_BASE
from .notation import concept_properties


def load_built(out_dir):
    """(url, version) -> {code: (display, properties)}, from every built release."""
    built = {}
    for path in sorted(out_dir.glob("CodeSystem-*.json")):
        cs = json.loads(path.read_text())
        # Skip MIMIC's own CodeSystems if they ever land here; only standard
        # releases are mapping targets.
        if cs["url"].startswith(MIMIC_BASE):
            continue
        built[(cs["url"], cs["version"])] = {
            c["code"]: (c.get("display", ""), concept_properties(c))
            for c in cs["concept"]
        }
        print(f"  loaded {path.name}: {cs['url']} v{cs['version']}, "
              f"{len(cs['concept']):,} concepts", file=sys.stderr)
    if not built:
        sys.exit(f"no CodeSystem-*.json in {out_dir} — run `make terminology` first")
    return built


def releases_of(built, system):
    """Every built release of a system, oldest first."""
    return sorted(v for (url, v) in built if url == system)


def find(built, tgt, official):
    """Earliest release of tgt['system'] holding `official`, honouring the
    target's kind filter and predicate. Returns (version, display) or None."""
    for version in releases_of(built, tgt["system"]):
        entry = built[(tgt["system"], version)].get(official)
        if entry is None:
            continue
        display, props = entry
        if tgt["kind"] and props.get("kind") != tgt["kind"]:
            continue
        if tgt["predicate"] and not tgt["predicate"](official, props):
            continue
        return version, display
    return None


_MTIME_FALLBACK_WARNED = False


def _git_date(paths):
    """The date this population's inputs last CHANGED, from git. None if unknown.

    Two calls, in this order, and the order is the whole design:

      status  any input dirty or untracked -> today. The content is not in
              history yet, so there is no commit that dates it, and claiming an
              older date would be a lie about bytes nobody has recorded.
      log     otherwise the commit date of the newest commit touching any of
              them, which is exactly "when did this map's inputs last move".

    One `git log` over all the paths rather than one per path: the answer wanted
    is the maximum, and that is what a multi-path log already returns.
    """
    if not paths:
        return None
    repo = paths[0].parent
    args = ["git", "-C", str(repo)]
    try:
        dirty = subprocess.run(
            [*args, "status", "--porcelain", "--", *map(str, paths)],
            capture_output=True, text=True, check=True).stdout.strip()
        if dirty:
            return datetime.date.today().isoformat()
        stamp = subprocess.run(
            [*args, "log", "-1", "--format=%cs", "--", *map(str, paths)],
            capture_output=True, text=True, check=True).stdout.strip()
        return stamp or None
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        return None


def default_date(out_dir, extra_paths=(), manifest=None):
    """The date this population's inputs last changed, from git.

    NOT today's date: these artefacts are committed, so a build from unchanged
    inputs must produce a byte-identical file. Dating them 'now' would churn the
    diff every day and make a real change indistinguishable from a re-run.

    NOT the newest input MTIME either, which is what this used to be. An mtime
    is not a property of the content: it does not survive a `git clone`, and a
    generator rewriting a file with identical bytes still moves it. Both failed
    in practice — a fresh clone moved EVERY date at once, `make units-table`
    moved the units map's date without changing a row, and the observation map's
    date tracked when `sushi .` last ran, because its newest inputs were
    gitignored build artefacts of another repo.

    Scoped to one population's inputs, where the single-file version considered
    every population's at once and so let a diagnosis edit move the procedure
    map's date.

    THE BUILT CODESYSTEMS ARE NOT AN INPUT HERE, though they are an input to the
    mapping. They are gitignored — 36-85 MB each, published as release assets —
    so git cannot date them, and their mtime says when someone last ran
    `make terminology` rather than when any code changed. What actually moves a
    map when a release changes is the release SET, and that is pinned by sha256
    in input-manifest.json, which IS committed. So the manifest stands in for
    them: it moves when the ICD inputs move, and not when someone rebuilds.

    Falls back to the old mtime behaviour, once and loudly, outside a git
    checkout — an exported tarball can still build, it just cannot promise the
    date means what it says.
    """
    global _MTIME_FALLBACK_WARNED
    inputs = list(extra_paths)
    if manifest is not None and manifest.is_file():
        inputs.append(manifest)

    stamp = _git_date(inputs)
    if stamp is not None:
        return stamp

    if not _MTIME_FALLBACK_WARNED:
        print("  not a git checkout — dating resources by input mtime, which "
              "does not survive a clone. `date` may churn without a real "
              "change.", file=sys.stderr)
        _MTIME_FALLBACK_WARNED = True
    newest = max((p.stat().st_mtime for p in inputs), default=0)
    return datetime.datetime.fromtimestamp(
        newest, datetime.timezone.utc).date().isoformat()
