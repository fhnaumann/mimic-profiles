#!/usr/bin/env python3
"""Vendor the IG resources this repo reads into ig-resources/.

WHY A SNAPSHOT AND NOT A PATH TO THE IG. Every source enumeration in this repo
is read from the MIMIC IG, never written out in a builder (see
conceptmaps/lib/igsource.py). Before this script that meant reading two
directories of a sibling checkout: `input/resources/` for the upstream MIMIC
CodeSystems, and `fsh-generated/resources/` for the FSH-authored ValueSets --
which are GITIGNORED, so they existed only if someone had run `sushi .` recently
enough. A build therefore depended on the state of another repo's working tree,
which is not a dependency a reproducible pipeline can have.

The snapshot makes the dependency explicit and reviewable: 45 files, one flat
directory, each pinned by sha256 in ig-resources/manifest.json. `make mappings`
then needs no Node, no SUSHI and no sibling checkout, and drift from the IG
becomes a diff on a manifest instead of an invisible difference in output.

THE CLOSURE IS DISCOVERED, NOT LISTED. A hand-kept file list is exactly the
registry this repo's stance argues against, and its failure mode is silent: a
forgotten file does not error, it makes a stream resolve against nothing and
report 0% coverage. So the set is computed from the two places that actually
declare what is read --

  conceptmaps/lib/streams.py    each stream's `file` / `valueset_file`
  occurrences/elements.json     each bound element's `bound_valuesets`

-- and then closed transitively over compose.include.valueSet and over bare
includes (a `system` with no `concept` means the whole CodeSystem, so the
CodeSystem is part of the closure too).

Usage:
  uv run sync_ig_resources.py --ig-dir ../mimic-profiles    refresh the snapshot
  uv run sync_ig_resources.py --check                       verify against hashes
  uv run sync_ig_resources.py --ig-dir ... --dry-run        report, write nothing
"""

import argparse
import datetime
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import paths  # noqa: E402

MANIFEST_NAME = "manifest.json"

# The two directories an IG checkout keeps its resources in, in the precedence
# igsource.resource_path used: input/resources/ first. They are snapshotted into
# ONE flat directory because their filenames do not collide (asserted below) and
# because the split is an artefact of how the IG is built, not a fact about the
# resources -- a consumer of a CodeSystem does not care whether a human wrote it
# or SUSHI did.
IG_SUBDIRS = ("input/resources", "fsh-generated/resources")


def warn(msg):
    print(f"  {msg}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# Discovery.
# --------------------------------------------------------------------------- #

def _index(source_dirs):
    """{canonical url: (path, resource)} over the IG's resource directories.

    First directory wins on a duplicate url, matching igsource.resource_path.
    """
    index, by_name = {}, {}
    for directory in source_dirs:
        if not directory.is_dir():
            warn(f"{directory} is not a directory -- skipped")
            continue
        for path in sorted(directory.glob("*.json")):
            try:
                resource = json.loads(path.read_text())
            except ValueError:
                continue
            if not isinstance(resource, dict) or "url" not in resource:
                continue
            index.setdefault(resource["url"], (path, resource))
            if path.name in by_name and by_name[path.name] != path:
                sys.exit(f"  {path.name} exists in more than one IG resource "
                         f"directory. The snapshot is flat, so the two would "
                         f"overwrite each other -- resolve the collision in the "
                         f"IG before syncing.")
            by_name.setdefault(path.name, path)
    return index, by_name


def _walk(url, index, found, seen=None):
    """Add `url`'s resource and everything its compose reaches to `found`."""
    seen = seen if seen is not None else set()
    if url in seen:
        return
    seen.add(url)
    entry = index.get(url)
    if entry is None:
        warn(f"{url} is in no IG resource directory -- nothing to snapshot")
        return
    path, resource = entry
    found.add(path)
    if resource.get("resourceType") == "CodeSystem":
        return
    for include in resource.get("compose", {}).get("include", []):
        for nested in include.get("valueSet", []):
            _walk(nested, index, found, seen)
        system = include.get("system")
        # A bare include -- a system with no enumerated concepts -- means the
        # whole CodeSystem, so the CodeSystem itself is part of the closure.
        if system and not include.get("concept"):
            _walk(system, index, found, seen)


def discover(source_dirs):
    """Every IG file this repo reads, transitively. Returns sorted [Path]."""
    index, by_name = _index(source_dirs)
    found = set()

    # 1. The files each stream names directly.
    from conceptmaps.lib import streams
    for name, stream in sorted(streams.STREAMS.items()):
        filename = stream.get("file") or stream.get("valueset_file")
        if not filename:
            continue
        path = by_name.get(filename)
        if path is None:
            sys.exit(f"  stream {name!r} names {filename}, which is in no IG "
                     f"resource directory. Run `sushi .` in the IG checkout, or "
                     f"fix the declaration in conceptmaps/lib/streams.py.")
        found.add(path)

    # 2. Every bound ValueSet, closed over its compose. This is what the
    #    completeness checks (verify check 5, streams.undeclared) expand.
    registry_path = paths.OCCURRENCES / "elements.json"
    registry = json.loads(registry_path.read_text())
    for element in registry["elements"]:
        for url in element.get("bound_valuesets", []):
            _walk(url, index, found)

    return sorted(found)


# --------------------------------------------------------------------------- #
# The manifest.
# --------------------------------------------------------------------------- #

def _sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _ig_provenance(ig_dir):
    """The IG checkout's commit and cleanliness, or {} outside a git tree."""
    def git(*args):
        try:
            return subprocess.run(("git", "-C", str(ig_dir), *args),
                                  capture_output=True, text=True,
                                  check=True).stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None

    commit = git("rev-parse", "HEAD")
    if commit is None:
        return {}
    return {"ig_commit": commit,
            "ig_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
            # A dirty IG tree means the snapshot records something that is in
            # nobody's history -- worth knowing when a number is questioned.
            "ig_dirty": bool(git("status", "--porcelain"))}


def _load_manifest(dest):
    path = dest / MANIFEST_NAME
    if not path.is_file():
        return {}
    return {f["path"]: f for f in json.loads(path.read_text()).get("files", [])}


def build_manifest(files, dest, ig_dir, previous, today):
    """The manifest entries, carrying content dates forward across a resync.

    `content_changed` is the date this repo FIRST SAW the current bytes, not the
    source file's mtime. mtime is the wrong signal twice over: the FSH-authored
    half of these files is regenerated by `sushi .`, so its mtime says when
    SUSHI last ran rather than when anything changed, and no mtime survives a
    git clone at all. A date that only moves when the sha256 moves is the one
    that means what a reader assumes it means.
    """
    entries = []
    for path in files:
        digest = _sha256(path)
        prior = previous.get(path.name, {})
        changed = (prior.get("content_changed", today)
                   if prior.get("sha256") == digest else today)
        entries.append({
            "path": path.name,
            "sha256": digest,
            "bytes": path.stat().st_size,
            # Which half of the IG it came from. `fsh-generated` means SUSHI
            # built it from input/fsh/, so it cannot be read out of a plain IG
            # checkout -- the reason this snapshot exists.
            "origin": ("fsh-generated" if "fsh-generated" in path.parts
                       else "input/resources"),
            "content_changed": changed,
        })
    return {
        "description": (
            "The MIMIC IG resources this repo reads. Discovered transitively "
            "from conceptmaps/lib/streams.py and occurrences/elements.json by "
            "sync_ig_resources.py -- never hand-edited. Refresh with "
            "`make sync-ig MIMIC_IG_DIR=...`; verify with `make verify-ig`."),
        "synced": today,
        "ig_dir": str(ig_dir),
        **_ig_provenance(ig_dir),
        "files": sorted(entries, key=lambda e: e["path"]),
    }


# --------------------------------------------------------------------------- #
# Commands.
# --------------------------------------------------------------------------- #

def sync(ig_dir, dest, dry_run):
    source_dirs = [ig_dir / sub for sub in IG_SUBDIRS]
    missing = [d for d in source_dirs if not d.is_dir()]
    if missing:
        sys.exit(f"  {missing[0]} does not exist. --ig-dir must point at a "
                 f"mimic-profiles checkout, and `sushi .` must have run there "
                 f"so fsh-generated/resources/ is populated.")

    files = discover(source_dirs)
    print(f"  {len(files)} file(s) in the closure "
          f"({sum(f.stat().st_size for f in files) / 1e6:.1f} MB)")

    previous = _load_manifest(dest)
    today = datetime.date.today().isoformat()
    manifest = build_manifest(files, dest, ig_dir, previous, today)

    changed = [e for e in manifest["files"]
               if previous.get(e["path"], {}).get("sha256") != e["sha256"]]
    removed = sorted(set(previous) - {e["path"] for e in manifest["files"]})
    for entry in changed:
        verb = "new" if entry["path"] not in previous else "changed"
        print(f"    {verb:8} {entry['path']}")
    for name in removed:
        print(f"    {'removed':8} {name}  (no longer in the closure)")
    if not changed and not removed:
        print("    no change")

    if dry_run:
        print("  --dry-run: nothing written")
        return 0

    dest.mkdir(parents=True, exist_ok=True)
    for path in files:
        shutil.copy2(path, dest / path.name)
    # A file that left the closure must leave the snapshot too, or it becomes a
    # resource nothing reads and nothing verifies.
    for name in removed:
        (dest / name).unlink(missing_ok=True)
    (dest / MANIFEST_NAME).write_text(json.dumps(manifest, indent=1) + "\n")
    print(f"  wrote {dest.name}/ and {MANIFEST_NAME}")
    return 0


def check(dest):
    """Verify the snapshot against its own manifest. Non-zero on any drift."""
    manifest_path = dest / MANIFEST_NAME
    if not manifest_path.is_file():
        sys.exit(f"  {manifest_path} not found -- run `make sync-ig` first.")
    manifest = json.loads(manifest_path.read_text())

    problems = []
    for entry in manifest["files"]:
        path = dest / entry["path"]
        if not path.is_file():
            problems.append(f"{entry['path']}: MISSING")
        elif _sha256(path) != entry["sha256"]:
            problems.append(f"{entry['path']}: sha256 mismatch")
    listed = {e["path"] for e in manifest["files"]}
    for path in sorted(dest.glob("*.json")):
        if path.name != MANIFEST_NAME and path.name not in listed:
            problems.append(f"{path.name}: present but not in the manifest")

    for problem in problems:
        print(f"  {problem}", file=sys.stderr)
    if problems:
        print(f"\n  {len(problems)} problem(s). The snapshot does not match its "
              f"manifest -- re-run `make sync-ig` against an IG checkout.",
              file=sys.stderr)
        return 1
    print(f"  {len(manifest['files'])} file(s) match the manifest "
          f"(IG commit {manifest.get('ig_commit', '?')[:8]}, "
          f"synced {manifest.get('synced', '?')})")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--ig-dir", type=Path,
                    help="mimic-profiles checkout to snapshot from "
                         "(default: $MIMIC_IG_DIR)")
    ap.add_argument("--dest", type=Path, default=paths.IG_RESOURCES,
                    help=f"snapshot directory (default: {paths.IG_RESOURCES})")
    ap.add_argument("--check", action="store_true",
                    help="verify the snapshot against its manifest and exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change, write nothing")
    args = ap.parse_args()

    if args.check:
        return check(args.dest)

    ig_dir = args.ig_dir or paths.MIMIC_IG_DIR
    if not ig_dir:
        sys.exit("  no --ig-dir and $MIMIC_IG_DIR is unset. Point it at a "
                 "mimic-profiles checkout that has had `sushi .` run.")
    return sync(Path(ig_dir).resolve(), args.dest, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
