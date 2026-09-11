"""Step 1 — convert the MIMIC-on-FHIR NDJSON distribution to a Pathling Delta warehouse.

MIMIC ships one NDJSON file per profile flavour (MimicObservationChartevents,
MimicMedicationAdministrationICU, ...). Pathling stores one Delta table per FHIR
resource type, so the flavours are collapsed: the demo dataset's 30 files become
13 tables.

Which resource type a file holds is read from the `resourceType` of its first
line, rather than matched off the file name as the Pathling MIMIC tutorial
does — same `file_name_mapper` mechanism, but no list of Mimic suffixes to
keep up to date, and no way for MimicMedicationMix -> Medication to go wrong.
The flavour is not lost; it stays on each resource's `meta.profile`.

This is the only step allowed to use the Pathling library directly, because it
creates the warehouse the server reads from. Later steps go through the server.

## Codecs, and why zstd is decompressed first

The DuckDB builds ship `.ndjson.zst`, which is the default here. **Spark cannot
read zstd at all** — the pyspark wheel carries no native Hadoop codecs, so a
read fails at execution time with `FAILED_READ_FILE`, after `resource_types()`
has already reported success. So `.zst` input is decompressed to plain NDJSON
first and Pathling reads that. Budget ~19x the compressed size for it: the
100-patient demo is 41 MB -> 767 MB, the full corpus 24 GiB -> 470 GiB.

Plain NDJSON is also the right choice at full scale for a second reason: gzip is
not splittable, so a single 12 GiB `MimicObservationChartevents.ndjson.gz` is
one Spark task on one core, while the uncompressed file is split across all of
them.

Usage:
  uv run python step1_ndjson_to_delta.py \
      --source /Users/nau025/warehouses/mimic-iv-demo \
      --target /Users/nau025/warehouses/mimic-iv-demo/delta

  # Full scale: stage somewhere with 470 GiB free, and keep it for a re-run.
  uv run python step1_ndjson_to_delta.py \
      --source .../mimic-iv-on-fhir-1.1.3-fixes_2026-08-22/duckdb-zstd/fhir \
      --target /scratch3/nau025/mimic-on-fhir-delta/spark_warehouse.new \
      --staging /scratch3/nau025/mimic-on-fhir-delta/staging-ndjson
"""

import argparse
import gzip
import json
import shutil
import subprocess
import sys
import tempfile
from contextlib import nullcontext
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)

# Extensions Spark reads in place. Anything else has to be staged.
READABLE_IN_PLACE = {"ndjson", "gz"}


def resource_type_of(path: Path) -> str | None:
    """The `resourceType` of the first line of a (possibly gzipped) NDJSON file."""
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as fh:
        line = fh.readline().strip()
    return json.loads(line).get("resourceType") if line else None


def decompress(source: Path, staging: Path) -> None:
    """`source`/*.zst -> `staging`/*.ndjson, via the zstd binary.

    The binary rather than the `zstandard` package: this repo does not depend on
    it, and zstd is present on both the laptop (homebrew) and Petrichor
    (/usr/bin/zstd). One less dependency for a step that shells out once per file.
    """
    if not shutil.which("zstd"):
        raise SystemExit("no `zstd` on PATH -- needed to decompress .zst input")

    files = sorted(source.glob("*.zst"))
    if not files:
        raise SystemExit(f"No *.zst files in {source}")

    # A half-finished previous run is the dangerous case: a truncated .ndjson
    # parses fine and is simply short, so the warehouse loses rows without
    # anything failing. Refuse rather than trust the sizes to catch it.
    staging.mkdir(parents=True, exist_ok=True)
    if any(staging.iterdir()):
        raise SystemExit(
            f"staging is not empty: {staging}\n"
            "Remove it deliberately before re-running -- a partial decompress "
            "leaves short files that parse fine and lose rows silently."
        )

    print(f"Decompressing {len(files)} file(s) → {staging} …")
    total = 0
    for path in files:
        out = staging / path.name[: -len(".zst")]
        subprocess.run(["zstd", "-dq", "--force", "-o", str(out), str(path)],
                       check=True)
        total += out.stat().st_size
    print(f"  staged {total:,d} bytes\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--source", type=Path, required=True,
                        help="directory of Mimic*.ndjson.zst (or .gz, or plain) files")
    parser.add_argument("--target", type=Path, required=True,
                        help="Delta warehouse to write the tables into")
    # Pathling globs on the *last* extension segment only, so this is "zst", not
    # "ndjson.zst" — the compound form matches no files. The mapper still gets
    # the fully stripped stem ('MimicPatient').
    parser.add_argument("--extension", default="zst",
                        help="last segment of the source file extension "
                             "(default: zst; `gz` and `ndjson` are read in place)")
    parser.add_argument("--staging", type=Path, default=None,
                        help="where to decompress .zst input (default: a temporary "
                             "directory, deleted on exit). Give an explicit path at "
                             "full scale -- 470 GiB -- to keep it for a re-run.")
    args = parser.parse_args()

    source, target = args.source.resolve(), args.target.resolve()

    # .zst is staged; everything else Spark opens where it lies.
    if args.extension in READABLE_IN_PLACE:
        staging_ctx = nullcontext(None)
    elif args.extension == "zst":
        staging_ctx = (nullcontext(str(args.staging)) if args.staging
                       else tempfile.TemporaryDirectory(prefix="mimic-ndjson-"))
    else:
        raise SystemExit(
            f"unsupported extension '{args.extension}': Spark reads ndjson and gz "
            "in place, this script stages zst, and nothing else is handled")

    with staging_ctx as staging:
        if staging is None:
            read_dir, read_extension = source, args.extension
        else:
            decompress(source, Path(staging))
            read_dir, read_extension = Path(staging).resolve(), "ndjson"

        suffix = f".{read_extension}"

        # Sniff every file up front, so the mapper is a plain dict lookup.
        by_stem = {}
        for path in sorted(p for p in read_dir.iterdir() if p.name.endswith(suffix)):
            resource_type = resource_type_of(path)
            if resource_type:
                by_stem[path.name.split(".")[0]] = [resource_type]
                print(f"  {path.name} → {resource_type}")
            else:
                print(f"  [warn] {path.name} is empty, skipping")
        if not by_stem:
            raise SystemExit(f"No *{suffix} files in {read_dir}")

        from pathling import PathlingContext

        print(f"\nConverting {len(by_stem)} file(s) → "
              f"{len(set(map(tuple, by_stem.values())))} table(s) in {target} …")
        # Extensions must be on: the server encodes with them (EncodingConfiguration
        # defaults enableExtensions=true), so a warehouse written without them lacks
        # the _fid / _extension columns the server's analysis expects, and every
        # ViewDefinition fails with DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS.
        pc = PathlingContext.create(
            enable_delta=True, enable_terminology=False, enable_extensions=True
        )

        # Pathling hands the mapper every entry in the directory with its extension
        # stripped, including the target warehouse when it is nested inside the
        # source. Anything unknown maps to no resource type, which excludes it.
        data = pc.read.ndjson(
            str(read_dir),
            extension=read_extension,
            file_name_mapper=lambda name: by_stem.get(name.split(".")[0], []),
        )
        resolved = data.resource_types()
        if not resolved:
            raise SystemExit(f"Pathling matched no *{suffix} files in {read_dir}")

        # save_mode="overwrite" replaces the tables this run writes and leaves every
        # other table in the warehouse alone -- which is what keeps a server's own
        # Library, ViewDefinition and jobs/ content intact through a data refresh.
        details = data.write.delta(str(target), save_mode="overwrite")

        print(f"\nWrote {len(details.file_infos)} table(s):")
        for info in sorted(details.file_infos, key=lambda fi: fi.fhir_resource_type):
            print(f"  {info.fhir_resource_type:28s} {info.absolute_url}")


if __name__ == "__main__":
    main()
