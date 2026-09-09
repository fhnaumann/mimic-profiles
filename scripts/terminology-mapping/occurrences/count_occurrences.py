#!/usr/bin/env python3
"""Count how often every coded value actually occurs in MIMIC-on-FHIR.

Runs ONCE on a CSIRO HPC node against the full Delta warehouse, and its output
files are then committed to this repo. Everything downstream is offline:
build_statistics.py reads code-occurrences.csv to weight the mapping coverage
tables by how much data each code carries, because missing a code used on every
admission is not the same failure as missing one used twice in 2012.

Why this and not the existing artefacts:

  scripts/binding-analysis/distinct-codes.ndjson already carries counts, but
  phase 0 filtered out every element that was ALREADY BOUND — so the three
  elements this repo maps most (Observation.code, Procedure.code, Specimen.type)
  are exactly the ones missing from it. Rather than merge two extraction runs
  against warehouse states that differ by a provisioning step, this recounts
  all ten elements in one job, with one provenance record.

  pathling_mcp_tool's get_cardinality_and_top_values_impl caps at 20 values,
  which is useless for a field whose whole story is its long tail, and it
  explodes multiple array columns independently, which cross-joins the codings
  of a multi-coding CodeableConcept instead of keeping them aligned. This uses
  the SQL-on-FHIR `forEach` that phase1_extract_distinct.py established: one row
  per Coding, system/code/display aligned by construction.

Deliberately self-contained: stdlib + pathling/pyspark only, no imports from
this repo. The node runs Python 3.12 under the REMOTE project's uv environment
(`/scratch3/nau025/mimic-on-fhir-delta`), not this repo's 3.14 one, and compute
nodes have no internet, so nothing here may resolve a dependency at run time.

Counts are keyed on (element, system, code) and NOT on meta.profile. A resource
may legitimately carry more than one profile, and grouping on an exploded
meta.profile would then count its codings once per profile. Population identity
is recoverable anyway: each unbuilt population has its own CodeSystem
(mimic-chartevents-d-items, mimic-d-labitems, mimic-microbiology-organism), so
the code's system says which population it belongs to without a profile column.

A CODING COUNT IS NOT A RESOURCE COUNT, and on a choice element the difference
is the whole story. `MedicationRequest.medication[x]` is `CodeableConcept |
Reference(Medication)`; only the CodeableConcept branch carries a Coding, so
extract_path can only ever see that branch. The 2026-08-06 run counted 1,883,681
codings against 15,416,901 MedicationRequest rows — so at least 87.8% of
requests carry their drug identity somewhere this file cannot reach, and a
coverage percentage over the codings alone reads as a statement about
prescriptions when it is a statement about one branch of one element.

Two extra views fix that, both declared per element in elements.json:

  count_shapes      per RESOURCE, which branch of the choice was taken. Turns
                    "silently absent" into a counted fact, and catches a
                    CodeableConcept carrying text but no coding — which under a
                    required binding is a conformance violation, not a shape.
  reference_join    follows the Reference to the element that DOES carry the
                    codes (Medication.code) and counts its codings weighted by
                    referring resources. That is the numerator for resource-level
                    reachability, and neither element can produce it alone.

Outputs, all written to --out-dir:
  code-occurrences.csv     element,system,code,display,occurrences
  element-shapes.csv       element,shape,resources — one row per observed
                           combination of the declared booleans. Only for
                           elements declaring count_shapes.
  reference-occurrences.csv  element,via,system,code,display,resources — the
                           target element's codings, counted once per REFERRING
                           resource. Only for elements declaring reference_join.
  occurrence-summary.json  per-element totals + the run's identity, including
                           each Delta table's version and commit timestamp and
                           the sha256 of the CSV. build_statistics.py verifies
                           that hash, so a half-copied CSV cannot quietly enter
                           a thesis table.

With --inventory-out, a SECOND export of the same counts in the storage-space
shape the cohort-selection orchestrator's code-pool add surface reads:

  <dataset>.csv            column,stored_system,stored_code,stored_display,
                           occurrences,subjects — resource-qualified column keys,
                           nullable exact patient counts.
  <dataset>.meta.json      per-column count grain and resource-grain totals, plus
                           the provenance identifying the snapshot.

`<dataset>` is DERIVED, not typed: --release plus a digest of the observed Delta
versions. A release label alone would have named both the warehouse counted on
2026-08-06 and the different one that replaced it on 2026-08-24.

Usage (on the node, via count_occurrences.slurm):
  uv run python3 occurrences/count_occurrences.py --data "$DATA" --out-dir occurrences
Dry run (laptop, no Spark, no warehouse):
  uv run python3 scripts/terminology-mapping/occurrences/count_occurrences.py --dry-run
"""

import argparse
import csv
import hashlib
import json
import os
import platform
import re
import socket
import sys
import traceback
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REGISTRY = os.path.join(SCRIPT_DIR, "elements.json")

CSV_NAME = "code-occurrences.csv"
SHAPES_NAME = "element-shapes.csv"
REFERENCES_NAME = "reference-occurrences.csv"
SUMMARY_NAME = "occurrence-summary.json"
CSV_COLUMNS = ["element", "system", "code", "display", "occurrences"]
SHAPES_COLUMNS = ["element", "shape", "resources"]
REFERENCES_COLUMNS = ["element", "via", "system", "code", "display",
                      "resources"]

# The label a shape row gets when every declared boolean came back false: the
# element is present (the resource is in the table) but took none of the branches
# that could carry a code. Spelled out rather than left as an empty string, which
# in a CSV is indistinguishable from a column nobody filled in.
NO_BRANCH = "(none)"


def log(msg):
    """Progress to stderr, so it interleaves with Spark's own stdout noise."""
    print(msg, file=sys.stderr, flush=True)


# --------------------------------------------------------------------------- #
# Query planning — pure, shared by the dry run and the real run.
# --------------------------------------------------------------------------- #

def plan_view(element):
    """The SQL-on-FHIR view for one element: one row per Coding.

    `forEach` over the Coding collection is what keeps system/code/display
    aligned per Coding. Reading them as three independent collection columns
    would cross-join them, which is wrong for any CodeableConcept carrying more
    than one Coding — and silently right for the ones carrying exactly one,
    which is how that bug survives review.

    Resources that do not have the element contribute no rows (forEach, not
    forEachOrNull): "absent" is not a coded value and must not become one.
    """
    return {
        "resource_type": element["resource_type"],
        "select": [{
            "forEach": element["extract_path"],
            "column": [
                {"path": "system", "name": "system"},
                {"path": "code", "name": "code"},
                {"path": "display", "name": "display"},
            ],
        }],
    }


def plan_shape_view(element):
    """The view for one element's choice-branch counts: one row per RESOURCE.

    No `forEach`, which is the entire point. A coding-level view cannot answer
    "how many resources took the Reference branch" because a resource that took
    it contributes no coding rows at all — it is absent from that view, and
    absence there is indistinguishable from a resource that has no such element.

    The booleans are declared in elements.json rather than derived from
    extract_path. Deriving them means string surgery on a FHIRPath to find the
    choice base, and a wrong guess yields plausible counts with nothing to catch
    them — the same failure mode the forEach comment above describes.
    """
    return {
        "resource_type": element["resource_type"],
        "select": [{
            "column": [{"path": path, "name": name}
                       for name, path in element["count_shapes"].items()],
        }],
    }


# How the reference join finds its partner rows. Two strategies, tried in order,
# because this job gets one queue slot and a wrong guess about FHIRPath support
# would cost a whole second run to learn.
#
#   reference-key  getReferenceKey() / getResourceKey(), the SQL-on-FHIR
#                  functions that exist for exactly this join. Correct by
#                  construction for every reference spelling.
#   string-id      the raw `reference` string against the target's `id`, with the
#                  trailing identifier extracted in Spark. Used only if the
#                  engine does not implement the functions above.
KEY_FUNCTIONS = "reference-key"
STRING_ID = "string-id"
JOIN_STRATEGIES = (KEY_FUNCTIONS, STRING_ID)


def plan_reference_views(element, target, strategy=KEY_FUNCTIONS):
    """(source view, target view) for a reference_join, joined on a resource key.

    The source view is one row per REFERRING resource, so the counts this
    produces are resource-weighted: a Medication referenced by 40,000 requests
    contributes 40,000, which is what makes the result comparable with the
    element's own resource total. The target view is one row per Coding, keyed so
    the join can fan out over a multi-coding Medication.code.

    `getReferenceKey()` is preferred over string-matching `Medication/xyz`
    against an id because a reference may legitimately be relative, absolute,
    versioned or a `urn:uuid:` — spellings a naive `split('/')` gets wrong in
    four different ways. The STRING_ID fallback handles all four too, but in
    Spark rather than in FHIRPath, and only because it must: see JOIN_STRATEGIES.
    """
    path = element["reference_join"]["reference_path"]
    if strategy == KEY_FUNCTIONS:
        source_column = {"path": f"{path}.getReferenceKey()",
                         "name": "target_key"}
        target_key = {"path": "getResourceKey()", "name": "target_key"}
    else:
        # Raw, unprocessed. The normalisation happens in count_references, where
        # Spark can do it — a FHIRPath expression complex enough to strip a
        # version suffix is exactly what this fallback exists to avoid needing.
        source_column = {"path": f"{path}.reference", "name": "target_ref"}
        target_key = {"path": "id", "name": "target_id"}
    return (
        {"resource_type": element["resource_type"],
         "select": [{"column": [source_column]}]},
        {"resource_type": target["resource_type"],
         "select": [
             {"column": [target_key]},
             {"forEach": target["extract_path"], "column": [
                 {"path": "system", "name": "system"},
                 {"path": "code", "name": "code"},
                 {"path": "display", "name": "display"},
             ]},
         ]},
    )


def load_registry(path, wanted=None):
    with open(path) as fh:
        elements = json.load(fh)["elements"]
    if wanted:
        keep = set(wanted)
        unknown = keep - {e["element"] for e in elements}
        if unknown:
            log(f"ERROR: unknown element(s): {', '.join(sorted(unknown))}")
            sys.exit(2)
        elements = [e for e in elements if e["element"] in keep]
    return elements


# --------------------------------------------------------------------------- #
# Dry run — must not import pyspark or pathling.
# --------------------------------------------------------------------------- #

def do_dry_run(elements, registry=None):
    registry = {e["element"]: e for e in (registry or elements)}
    log(f"DRY RUN: {len(elements)} element(s) planned\n")
    for i, element in enumerate(elements, 1):
        plan = plan_view(element)
        print("=" * 70)
        print(f"[{i}] {element['element']}")
        print(f"    resource : {plan['resource_type']}")
        print(f"    forEach  : {element['extract_path']}")
        print(f"    select   : {json.dumps(plan['select'])}")
        print("    groupBy  : system, code   (display via max, for determinism)")
        if element.get("count_shapes"):
            shape = plan_shape_view(element)
            print(f"    + shapes : {json.dumps(shape['select'])}")
            print("      groupBy: every declared boolean   -> "
                  f"{SHAPES_NAME}")
        if element.get("reference_join"):
            name = element["reference_join"]["target"]
            target = registry.get(name)
            if target is None:
                print(f"    + refjoin: ERROR unknown target element {name!r}")
            else:
                for strategy in JOIN_STRATEGIES:
                    src, tgt = plan_reference_views(element, target, strategy)
                    print(f"    + refjoin: {src['resource_type']} -> "
                          f"{tgt['resource_type']} ({name})  [{strategy}]")
                    print(f"      source : {json.dumps(src['select'])}")
                    print(f"      target : {json.dumps(tgt['select'])}")
                print("      join   : LEFT on target_key, so an unresolvable "
                      f"reference is counted rather than dropped. Strategies "
                      f"tried in the order shown   -> {REFERENCES_NAME}")
    print("=" * 70)
    log("\nDRY RUN complete. No Spark session was created.")
    return 0


# --------------------------------------------------------------------------- #
# Real run.
# --------------------------------------------------------------------------- #

def open_warehouse(data_path):
    """PathlingContext + the Delta warehouse.

    PathlingContext must build its own SparkSession — a pre-existing one lacks
    the Pathling JVM libraries on the classpath, which surfaces as the
    thoroughly unhelpful "TypeError: 'JavaPackage' object is not callable".
    """
    from pathling import PathlingContext

    pc = PathlingContext.create()
    return pc, pc.read.delta(data_path)


def delta_identity(spark, data_path, resource_type):
    """(version, commit timestamp, rows) for one resource's Delta table.

    A real fingerprint of what was counted, rather than a path someone typed.
    Best-effort: a warehouse laid out differently still produces correct counts,
    so a failure here records nulls instead of losing the run.
    """
    table = f"{data_path.rstrip('/')}/{resource_type}.parquet"
    try:
        history = spark.sql(f"DESCRIBE HISTORY delta.`{table}`") \
            .select("version", "timestamp") \
            .orderBy("version", ascending=False) \
            .first()
        rows = spark.sql(f"SELECT count(*) AS n FROM delta.`{table}`").first()["n"]
        return {"delta_version": history["version"],
                "committed": str(history["timestamp"]),
                "rows": rows}
    except Exception as exc:  # noqa: BLE001 — provenance is not worth a crash
        return {"error": "".join(
            traceback.format_exception_only(type(exc), exc)).strip()}


def count_element(data, element):
    """[(system, code, display, occurrences)] for one element, plus its totals.

    Grouped on (system, code) alone, with `display` taken as max() rather than
    first(): a code whose display drifted across the ETL would otherwise split
    into two rows, inflating the distinct-code count, and first() without an
    ORDER BY would make the committed CSV non-deterministic.
    """
    from pyspark.sql import functions as F

    plan = plan_view(element)
    df = data.view(plan["resource_type"], select=plan["select"])
    agg = (df.groupBy("system", "code")
             .agg(F.count(F.lit(1)).alias("occurrences"),
                  F.max("display").alias("display"))
             .orderBy(F.desc("occurrences"), "system", "code"))
    rows = [{"element": element["element"],
             "system": r["system"] or "",
             "code": r["code"] or "",
             "display": r["display"] or "",
             "occurrences": r["occurrences"]}
            for r in agg.collect()]
    return rows, {
        # Codings, not resources: this is the denominator every occurrence-
        # weighted percentage downstream divides by, so it is what gets stated.
        "codings": sum(r["occurrences"] for r in rows),
        "distinct_codes": len(rows),
        # A Coding present but carrying no code — a data defect, and one that
        # would otherwise hide inside an empty-string group.
        "codings_without_code": sum(r["occurrences"] for r in rows
                                    if not r["code"]),
    }


def count_shapes(data, element):
    """[(shape, resources)] for one element's choice branches, plus its totals.

    `shape` names the branches that were TRUE, joined with `+`, so one row is
    readable on its own: `with_codeable_concept+with_coding` is an inline coded
    concept and `with_reference` is a pointer. Reporting a column per boolean
    instead would give every element a different CSV header.

    The combination that matters most is the one nobody plans for:
    with_codeable_concept true and with_coding false is a CodeableConcept
    carrying text alone, which under a REQUIRED binding is non-conformant — R4
    asks for at least one Coding from the value set, and text is not one. It is
    surfaced as `codeable_concept_without_coding` rather than left for a reader
    to reconstruct from the shape labels.
    """
    from pyspark.sql import functions as F

    names = list(element["count_shapes"])
    plan = plan_shape_view(element)
    df = data.view(plan["resource_type"], select=plan["select"])
    agg = (df.groupBy(*names)
             .agg(F.count(F.lit(1)).alias("resources"))
             .orderBy(F.desc("resources"), *names))

    rows, totals = [], {name: 0 for name in names}
    resources = no_branch = text_only = 0
    for record in agg.collect():
        n = record["resources"]
        true_names = [name for name in names if record[name]]
        rows.append({"element": element["element"],
                     "shape": "+".join(true_names) or NO_BRANCH,
                     "resources": n})
        resources += n
        for name in true_names:
            totals[name] += n
        if not true_names:
            no_branch += n
        if ("with_codeable_concept" in names and "with_coding" in names
                and record["with_codeable_concept"]
                and not record["with_coding"]):
            text_only += n

    summary = {"resources": resources, **totals, "no_branch": no_branch}
    if "with_codeable_concept" in names and "with_coding" in names:
        summary["codeable_concept_without_coding"] = text_only
    return rows, summary


def count_references(data, element, target):
    """[(system, code, display, resources)] reached THROUGH the reference, + totals.

    Counted once per REFERRING resource, not once per target resource: the
    question is "how many MedicationRequests point at a drug code $translate can
    resolve", and a Medication referenced by 40,000 prescriptions answers it
    40,000 times. Aggregating the target alone would answer a question about the
    drug dictionary instead, which the target element's own count already covers.

    A LEFT join, deliberately. An inner join would silently drop a reference
    that resolves to nothing, and a dangling reference in a warehouse whose
    required binding is only enforceable on the target is precisely the defect
    worth counting rather than hiding — it is reported as `dangling`.
    """
    last_error = None
    for strategy in JOIN_STRATEGIES:
        try:
            return _count_references(data, element, target, strategy)
        except Exception as exc:  # noqa: BLE001 — see JOIN_STRATEGIES
            last_error = exc
            err = "".join(
                traceback.format_exception_only(type(exc), exc)).strip()
            log(f"      refjoin: {strategy} strategy failed ({err})")
    raise last_error


def _count_references(data, element, target, strategy):
    """One attempt at the reference join, with one key strategy.

    Split out so a strategy that the engine does not support costs a retry rather
    than the whole output. The chosen strategy is recorded in the summary, because
    "which join produced this" is provenance a reader of the numbers needs and
    cannot recover afterwards.
    """
    from pyspark.sql import functions as F

    src_plan, tgt_plan = plan_reference_views(element, target, strategy)
    src = data.view(src_plan["resource_type"], select=src_plan["select"])
    tgt = data.view(tgt_plan["resource_type"], select=tgt_plan["select"])

    if strategy == STRING_ID:
        # Normalised in Spark rather than in FHIRPath — a FHIRPath expression
        # complex enough to strip a version suffix is what this fallback exists
        # to avoid needing.
        # An empty extraction is not a key; `_reference_to_key` nulls it, so it
        # lands in `dangling` rather than joining every unmatched row to every
        # other.
        src = _reference_to_key(src, "target_ref", "target_key")
        tgt = tgt.withColumnRenamed("target_id", "target_key")

    src = src.where(F.col("target_key").isNotNull())
    referring = src.count()
    joined = src.join(tgt, on="target_key", how="left")
    agg = (joined.groupBy("system", "code")
                 .agg(F.count(F.lit(1)).alias("resources"),
                      F.max("display").alias("display"))
                 .orderBy(F.desc("resources"), "system", "code"))

    rows, dangling = [], 0
    for record in agg.collect():
        if record["code"] is None and record["system"] is None:
            # Either the reference resolved to no Medication at all or it
            # resolved to one carrying no code. Both leave the referring
            # resource with no translatable code, which is the fact being
            # counted; telling them apart needs the target's own row count and
            # is left to the reader of occurrence-summary.json.
            dangling += record["resources"]
            continue
        rows.append({"element": element["element"],
                     "via": target["element"],
                     "system": record["system"] or "",
                     "code": record["code"] or "",
                     "display": record["display"] or "",
                     "resources": record["resources"]})
    return rows, {
        "via": target["element"],
        "join_strategy": strategy,
        "referring_resources": referring,
        "codings_reached": sum(r["resources"] for r in rows),
        "distinct_codes_reached": len(rows),
        "dangling": dangling,
    }


# --------------------------------------------------------------------------- #
# Per-resource shape and direct-patient counts.
#
# Both exist for the storage-space inventory (see the inventory section below),
# which has to state resource-grain totals and a patient count that is either
# exact or absent. Neither is derivable from the coding-level counts above:
# subtracting codings from resources is not a resource count on any element that
# can carry more than one coding, and a coding-level view has no subject column.
# --------------------------------------------------------------------------- #

def count_resource_shape(data, element):
    """{total_resources, uncoded_resources} for one element.

    One row per resource — no `forEach` — so the row count IS the table's row
    count and `coded_exists` is evaluated once per resource. `uncoded_resources`
    is therefore resources with no coding at this element, not
    `resources - codings`, which for `Observation.component.code` would be
    negative long before it was wrong in any interesting way.
    """
    from pyspark.sql import functions as F

    df = data.view(element["resource_type"], select=[{
        "column": [{"path": element["coded_exists"], "name": "coded"}],
    }])
    df = df.select(F.coalesce(F.col("coded"), F.lit(False)).alias("coded")).cache()
    try:
        total = df.count()
        uncoded = df.where(~F.col("coded")).count()
    finally:
        df.unpersist()
    return {"total_resources": total, "uncoded_resources": uncoded}


def plan_subject_view(element, strategy=KEY_FUNCTIONS):
    """The view for one element's per-coding subject keys.

    A resource-level column select beside the coding `forEach`, which is the
    shape `plan_reference_views` already relies on: the subject key repeats down
    every coding of its resource, so a distinct count over it is a distinct
    patient count for that stored identity.
    """
    path = element["subject_path"]
    if strategy == KEY_FUNCTIONS:
        subject = {"path": f"{path}.getReferenceKey()", "name": "subject_key"}
    else:
        subject = {"path": f"{path}.reference", "name": "subject_ref"}
    return {
        "resource_type": element["resource_type"],
        "select": [
            {"column": [subject]},
            {"forEach": element["extract_path"], "column": [
                {"path": "system", "name": "system"},
                {"path": "code", "name": "code"},
            ]},
        ],
    }


def count_subjects(data, element):
    """({(system, code): subjects|None}, totals) — exact distinct patients only.

    `subjects` is None for any stored identity with even one coding occurrence
    whose subject key is absent: the distinct count over the rest is a LOWER
    BOUND, and a lower bound published in an integer column is read as a fact.
    Null is the honest answer and the inventory's `subjects` column is nullable
    for exactly this reason.

    Same two key strategies, same order, same rationale as JOIN_STRATEGIES.
    """
    last_error = None
    for strategy in JOIN_STRATEGIES:
        try:
            return _count_subjects(data, element, strategy)
        except Exception as exc:  # noqa: BLE001 — see JOIN_STRATEGIES
            last_error = exc
            err = "".join(
                traceback.format_exception_only(type(exc), exc)).strip()
            log(f"      subjects: {strategy} strategy failed ({err})")
    raise last_error


def _count_subjects(data, element, strategy):
    from pyspark.sql import functions as F

    plan = plan_subject_view(element, strategy)
    df = data.view(plan["resource_type"], select=plan["select"])
    if strategy == STRING_ID:
        df = _reference_to_key(df, "subject_ref", "subject_key")

    agg = df.groupBy("system", "code").agg(
        F.count_distinct("subject_key").alias("subjects"),
        F.sum(F.when(F.col("subject_key").isNull(), 1).otherwise(0))
         .alias("without_subject"))

    subjects, incomplete, exact = {}, 0, 0
    for record in agg.collect():
        key = (record["system"] or "", record["code"] or "")
        if record["without_subject"]:
            subjects[key] = None
            incomplete += 1
        else:
            subjects[key] = record["subjects"]
            exact += 1
    return subjects, {
        "subject_basis": f"{element['subject_path']}.getReferenceKey()"
                         if strategy == KEY_FUNCTIONS
                         else f"{element['subject_path']}.reference",
        "key_strategy": strategy,
        "identities_with_exact_subjects": exact,
        "identities_without_complete_association": incomplete,
    }


def _reference_to_key(df, ref_column, key_column):
    """Trailing logical id of a reference string, in Spark.

    Shared by the reference join and the subject count so the two cannot
    normalise `Patient/abc` and `urn:uuid:abc` differently.

    Two steps, not `split('/')`. Strip any `/_history/N` first — otherwise the
    version number becomes the id — then take the trailing run of characters
    that is neither `/` nor `:`, which yields the logical id for all four
    spellings: `Patient/abc`, `http://h/fhir/Patient/abc`, `urn:uuid:abc`, and
    `Patient/abc/_history/2`.
    """
    from pyspark.sql import functions as F

    out = df.withColumn(
        key_column,
        F.regexp_extract(
            F.regexp_replace(F.col(ref_column), r"/_history/.*$", ""),
            r"([^/:]+)$", 1))
    if ref_column != key_column:
        out = out.drop(ref_column)
    return out.withColumn(key_column,
                          F.when(F.col(key_column) == "", None)
                           .otherwise(F.col(key_column)))


def write_csv(rows, out_dir):
    """One CSV for every element, ordered by element then descending count.

    Descending count is the order a reader wants (the heaviest codes first) and
    is fully determined by the data, so the committed file is stable.
    """
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, CSV_NAME)
    rows = sorted(rows, key=lambda r: (r["element"], -r["occurrences"],
                                       r["system"], r["code"]))
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_side_csv(rows, out_dir, name, columns, sort_key):
    """One of the two per-element side files, or nothing if no element wants it.

    Not written at all when empty, rather than written as a header-only file: a
    header with no rows says "measured, found nothing", and for these two files
    the truthful statement is "no element declared this", which is a different
    thing. common/occurrences.py treats a missing file as the latter.
    """
    if not rows:
        return None
    path = os.path.join(out_dir, name)
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(sorted(rows, key=sort_key))
    return path


def sha256(path):
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


# --------------------------------------------------------------------------- #
# The storage-space inventory.
#
# A SECOND export of the same extraction, in the shape the cohort-selection
# orchestrator's code-pool add surface reads (see that repo's AGENTS.md §7 and
# agent_plans/code-pool-add-surface/02-occurrence-artifact.md). Everything below
# is pure over the collected counts, so the whole artifact contract is testable
# without Spark, a warehouse, or a queue slot — which matters when the only way
# to observe the real thing is a job that runs once.
#
# It is deliberately STORAGE-SPACE only: observed `(column, system, code)` with
# counts and labels. No target codes, no reachability classification, no
# translation policy. Classifying a stored code as reachable needs the
# ConceptMaps and the consumer's equivalence policy, and a generator that
# reimplemented that policy would be a second, silently divergent copy of it.
# --------------------------------------------------------------------------- #

INVENTORY_SCHEMA_VERSION = 1
INVENTORY_COLUMNS = ["column", "stored_system", "stored_code", "stored_display",
                     "occurrences", "subjects"]

# `dataset` becomes a bare filename in the consuming repo, and that repo
# validates the same shape at its config boundary. Duplicated rather than
# shared: the two repos have no common package, and the failure mode of a label
# that only one side accepts is a build that cannot be configured — loud, and
# far better than a path-traversing label that both sides pass along.
SAFE_LABEL_RE = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"

EVENT_CODING = "event_coding"
DICTIONARY_CODING = "dictionary_coding"


def scope_key(element):
    """`MedicationRequest.medication[x]` → `MedicationRequest.medication`.

    The consumer keys its column configuration on the CodeableConcept element
    with the choice marker dropped, so the `[x]` is normalised HERE, once,
    rather than left for a reader to guess whether `medication[x]` and
    `medication` are the same column. Resource identity comes from the declared
    element name, never from a joined table name.
    """
    return element["element"].replace("[x]", "")


def occurrence_kind(element):
    return (DICTIONARY_CODING if element.get("occurrence_kind") == "dictionary"
            else EVENT_CODING)


def snapshot_digest(tables):
    """A digest over the OBSERVED Delta identity of every table read.

    This, not the release label, is what identifies a snapshot. `mimic-iv-3.1`
    named both the warehouse counted on 2026-08-06 (Delta versions 2-3) and the
    one that replaced it on 2026-08-24 (every table rewritten to version 0) —
    a release label reads as unchanged across a reload that changed everything.

    Raises when any table's identity failed to read: an inventory that cannot
    say what it counted must not be given a name suggesting it can.
    """
    missing = sorted(name for name, ident in tables.items()
                     if "delta_version" not in ident)
    if missing:
        raise ValueError(
            "cannot derive a snapshot identity: no Delta version/timestamp for "
            + ", ".join(missing))
    payload = [[name, tables[name]["delta_version"], tables[name]["committed"]]
               for name in sorted(tables)]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def derive_dataset_label(release, tables):
    """`<release>-<snapshot digest prefix>`, the committed inventory's filename.

    Derived from build output rather than typed, per the owner decision: the
    release is an input, the snapshot identity is a measurement.
    """
    if not re.fullmatch(SAFE_LABEL_RE, release or ""):
        raise ValueError(f"--release {release!r} is not a safe label "
                         f"(expected {SAFE_LABEL_RE})")
    label = f"{release}-{snapshot_digest(tables)[:12]}"
    if not re.fullmatch(SAFE_LABEL_RE, label):
        raise ValueError(f"derived dataset label {label!r} is too long; "
                         f"shorten --release")
    return label


def load_label_sources(paths):
    """({(system, code): display}, provenance) from CodeSystem/ValueSet JSON.

    DICTIONARY LABELS ONLY. These files enumerate codes and their designations;
    they are not ConceptMaps and nothing here reads a translation. The join key
    is `(system, code)`, never a bare code — the same code means different
    things in two MIMIC CodeSystems, and a bare-code join would silently label
    one with the other's display.

    Provenance is a CONTENT DIGEST per file, not a repo revision: the mapping
    outputs in this repo are regenerated in the working tree, so a commit hash
    does not identify what was actually read.
    """
    labels, provenance = {}, []
    for path in _expand_label_paths(paths):
        with open(path) as fh:
            resource = json.load(fh)
        added = 0
        for system, code, display in _iter_labelled_concepts(resource):
            if not system or not code or not display:
                continue
            # First file wins, so the result does not depend on directory order
            # beyond the caller's own explicit ordering.
            if labels.setdefault((system, code), display) == display:
                added += 1
        provenance.append({
            "file": os.path.basename(path),
            "sha256": sha256(path),
            "resource_type": resource.get("resourceType"),
            "url": resource.get("url"),
            "version": resource.get("version"),
            "labels": added,
        })
    return labels, provenance


def _expand_label_paths(paths):
    out = []
    for path in paths or ():
        if os.path.isdir(path):
            out.extend(os.path.join(path, name)
                       for name in sorted(os.listdir(path))
                       if name.endswith(".json"))
        else:
            out.append(path)
    return out


def _iter_labelled_concepts(resource):
    """(system, code, display) from a CodeSystem or an expanded ValueSet.

    A CodeSystem's concepts inherit its own `url` as their system; a ValueSet's
    `compose.include` and `expansion.contains` entries carry their own. Nested
    CodeSystem concepts are walked, because MIMIC's are flat today and a
    hierarchy appearing later should not silently drop labels.
    """
    kind = resource.get("resourceType")
    if kind == "CodeSystem":
        system = resource.get("url")
        stack = list(resource.get("concept") or ())
        while stack:
            concept = stack.pop()
            stack.extend(concept.get("concept") or ())
            yield system, concept.get("code"), concept.get("display")
    elif kind == "ValueSet":
        for include in resource.get("compose", {}).get("include") or ():
            system = include.get("system")
            for concept in include.get("concept") or ():
                yield system, concept.get("code"), concept.get("display")
        for contains in resource.get("expansion", {}).get("contains") or ():
            yield (contains.get("system"), contains.get("code"),
                   contains.get("display"))


def build_inventory(elements, code_rows, subjects, subject_basis, shapes, labels):
    """(csv rows, per-column manifest entries) for the storage-space artifact.

    Pure. `code_rows` is `count_element`'s output for every element, `subjects`
    and `shapes` are keyed by element name, `labels` is `load_label_sources`'s
    first return.

    Rows with no system or no code are INVALID IDENTITIES, not codes: they are
    excluded from the CSV — the consumer keys on `(column, system, code)` and
    cannot address them — but counted in the manifest, so
    `sum(occurrences) + invalid_identity_coding_occurrences` reconciles with the
    coding total rather than quietly shrinking the denominator.

    A column whose resource-grain measurement is missing raises. Those totals
    are what stop a coding count being read as a resource or patient volume, so
    an artifact without them is not a partial artifact, it is a misleading one.
    """
    by_element = {}
    for row in code_rows:
        by_element.setdefault(row["element"], []).append(row)

    rows, columns, incomplete = [], {}, []
    for element in elements:
        name = element["element"]
        column = scope_key(element)
        shape = shapes.get(name) or {}
        if not isinstance(shape.get("total_resources"), int) \
                or not isinstance(shape.get("uncoded_resources"), int):
            incomplete.append(name)
            continue

        element_subjects = subjects.get(name) or {}
        codes = invalid = occurrences = 0
        for row in by_element.get(name, ()):
            occurrences += row["occurrences"]
            if not row["system"] or not row["code"]:
                invalid += row["occurrences"]
                continue
            key = (row["system"], row["code"])
            rows.append({
                "column": column,
                "stored_system": row["system"],
                "stored_code": row["code"],
                # An unlabelled code stays searchable BY CODE. Neither invented
                # nor fatal to the column: MIMIC populates `.display` on about
                # half of these elements, so requiring a label would drop
                # `Medication.code` entirely.
                "stored_display": (row["display"]
                                   or labels.get(key)
                                   or ""),
                "occurrences": row["occurrences"],
                "subjects": element_subjects.get(key),
            })
            codes += 1

        columns[column] = {
            "codes": codes,
            "coding_occurrences": occurrences,
            "invalid_identity_coding_occurrences": invalid,
            "total_resources": shape["total_resources"],
            "uncoded_resources": shape["uncoded_resources"],
            "occurrence_kind": occurrence_kind(element),
            # The documented extraction path, or null. Null is not "no patients
            # exist"; it is "this element has no direct patient reference", which
            # for Medication is a fact about a drug dictionary.
            "subject_basis": subject_basis.get(name),
        }

    if incomplete:
        raise ValueError(
            "no resource-grain measurement (total_resources/uncoded_resources) "
            "for: " + ", ".join(sorted(incomplete))
            + " — refusing to write an inventory whose coding counts have no "
              "resource denominator")
    return rows, columns


def write_inventory(rows, columns, out_dir, dataset, provenance):
    """Write `<dataset>.csv` + `<dataset>.meta.json`, and return both paths.

    `subjects` is written as an EMPTY FIELD when unavailable, never `0`. A
    reader that cannot tell "not measured" from "no patients" will publish the
    second, and the whole point of the nullable column is that the first is
    common and the second never happens.
    """
    seen = set()
    for row in rows:
        key = (row["column"], row["stored_system"], row["stored_code"])
        if key in seen:
            raise ValueError(f"duplicate inventory identity {key}")
        seen.add(key)

    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, f"{dataset}.csv")
    rows = sorted(rows, key=lambda r: (r["column"], -r["occurrences"],
                                       r["stored_system"], r["stored_code"]))
    # LF, not csv's RFC-4180 default of CRLF: csv_sha256 is over the file's
    # bytes, and git normalizes CRLF to LF on checkout, so a CRLF artifact fails
    # its own checksum in every consumer that clones the repo it ships in.
    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=INVENTORY_COLUMNS,
                                extrasaction="raise", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["subjects"] = "" if out["subjects"] is None else out["subjects"]
            writer.writerow(out)

    manifest = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "dataset": dataset,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "csv": os.path.basename(csv_path),
        "csv_sha256": sha256(csv_path),
        "rows": len(rows),
        # The digest proves FILE identity — a changed or truncated CSV — and the
        # dataset stamp catches a wrong configured artifact. Neither can detect
        # a warehouse reloaded under an unchanged configuration, which has
        # already happened once to this repo's own committed counts. This is not
        # a proof of current live warehouse contents.
        "identity_limitation":
            "csv_sha256 proves file identity and `dataset` proves which snapshot "
            "was counted; neither proves the live server still holds it.",
        "columns": columns,
        **provenance,
    }
    meta_path = os.path.join(out_dir, f"{dataset}.meta.json")
    with open(meta_path, "w") as fh:
        json.dump(manifest, fh, indent=1, sort_keys=True)
        fh.write("\n")
    return csv_path, meta_path


def do_run(elements, data_path, out_dir, registry=None, inventory_out=None,
           release=None, label_paths=()):
    if not data_path:
        log("ERROR: --data is required for a real run (or use --dry-run).")
        return 2

    log(f"Opening Delta warehouse: {data_path}")
    pc, data = open_warehouse(data_path)

    # The FULL registry, not the (possibly --elements filtered) run list: a
    # reference_join names its target by element, and that target has to be
    # resolvable even when the target itself is not being recounted this run.
    by_name = {e["element"]: e for e in (registry or elements)}

    all_rows = []
    shape_rows = []
    reference_rows = []
    per_element = {}
    tables = {}
    subjects = {}
    subject_basis = {}
    shapes = {}
    failures = 0

    for i, element in enumerate(elements, 1):
        name = element["element"]
        resource_type = element["resource_type"]
        if resource_type not in tables:
            tables[resource_type] = delta_identity(pc.spark, data_path,
                                                   resource_type)
        try:
            rows, totals = count_element(data, element)
            all_rows.extend(rows)
            per_element[name] = totals
            log(f"[{i}/{len(elements)}] {name} -> "
                f"{totals['distinct_codes']:,} codes, "
                f"{totals['codings']:,} codings")
        except Exception as exc:  # noqa: BLE001 — one bad element must not
            # cost the whole job: this runs on a queue slot, and six good
            # elements are worth keeping when the seventh has a bad path.
            err = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            per_element[name] = {"error": err}
            failures += 1
            log(f"[{i}/{len(elements)}] {name} -> ERROR: {err}")

        # The resource-grain shape, for every element. Unlike the two optional
        # side views below, the inventory export REFUSES to write a column
        # without it (see build_inventory), because a coding count with no
        # resource denominator is the number people misread.
        try:
            shapes[name] = count_resource_shape(data, element)
            per_element.setdefault(name, {})["resource_shape"] = shapes[name]
            log(f"      shape -> {shapes[name]['total_resources']:,} resources, "
                f"{shapes[name]['uncoded_resources']:,} uncoded")
        except Exception as exc:  # noqa: BLE001
            err = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            per_element.setdefault(name, {})["resource_shape"] = {"error": err}
            failures += 1
            log(f"      shape -> ERROR: {err}")

        if element.get("subject_path"):
            try:
                subjects[name], totals = count_subjects(data, element)
                subject_basis[name] = totals["subject_basis"]
                per_element.setdefault(name, {})["subjects"] = totals
                log(f"      subjects -> {totals['identities_with_exact_subjects']:,} "
                    f"exact, {totals['identities_without_complete_association']:,} "
                    f"without complete association")
            except Exception as exc:  # noqa: BLE001
                err = "".join(
                    traceback.format_exception_only(type(exc), exc)).strip()
                per_element.setdefault(name, {})["subjects"] = {"error": err}
                failures += 1
                log(f"      subjects -> ERROR: {err}")

        # Both side views are OPTIONAL and each fails independently of the code
        # count above. They are the newest queries here and the ones most likely
        # to meet a FHIRPath a given Pathling build does not implement
        # (getReferenceKey in particular); losing the per-code counts — the
        # artifact everything downstream needs — to a failure in a supplementary
        # view would be a bad trade on a queue slot.
        if element.get("count_shapes"):
            try:
                rows, totals = count_shapes(data, element)
                shape_rows.extend(rows)
                per_element.setdefault(name, {})["shapes"] = totals
                log(f"      shapes -> {totals['resources']:,} resources in "
                    f"{len(rows)} shape(s)")
            except Exception as exc:  # noqa: BLE001
                err = "".join(
                    traceback.format_exception_only(type(exc), exc)).strip()
                per_element.setdefault(name, {})["shapes"] = {"error": err}
                failures += 1
                log(f"      shapes -> ERROR: {err}")

        if element.get("reference_join"):
            target_name = element["reference_join"]["target"]
            target = by_name.get(target_name)
            if target is None:
                err = f"reference_join target {target_name!r} is not in the registry"
                per_element.setdefault(name, {})["reference_join"] = {"error": err}
                failures += 1
                log(f"      refjoin -> ERROR: {err}")
            else:
                try:
                    rows, totals = count_references(data, element, target)
                    reference_rows.extend(rows)
                    per_element.setdefault(name, {})["reference_join"] = totals
                    log(f"      refjoin -> {totals['referring_resources']:,} "
                        f"referring, {totals['codings_reached']:,} codings "
                        f"reached via {target_name}, "
                        f"{totals['dangling']:,} dangling")
                except Exception as exc:  # noqa: BLE001
                    err = "".join(
                        traceback.format_exception_only(type(exc), exc)).strip()
                    per_element.setdefault(name, {})["reference_join"] = {
                        "error": err}
                    failures += 1
                    log(f"      refjoin -> ERROR: {err}")

    csv_path = write_csv(all_rows, out_dir)
    shapes_path = write_side_csv(
        shape_rows, out_dir, SHAPES_NAME, SHAPES_COLUMNS,
        lambda r: (r["element"], -r["resources"], r["shape"]))
    references_path = write_side_csv(
        reference_rows, out_dir, REFERENCES_NAME, REFERENCES_COLUMNS,
        lambda r: (r["element"], r["via"], -r["resources"], r["system"],
                   r["code"]))
    summary = {
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": socket.gethostname(),
        "warehouse": data_path,
        "csv": CSV_NAME,
        "csv_sha256": sha256(csv_path),
        "versions": {
            "python": platform.python_version(),
            "spark": pc.spark.version,
            "pathling": _pathling_version(),
        },
        "tables": tables,
        "elements": per_element,
    }
    # Hashed like the main CSV and for the same reason: build_statistics.py
    # refuses counts whose bytes it cannot vouch for, and a side file that
    # bypassed that check would be the one number in the thesis nothing verified.
    for key, path in (("shapes_csv", shapes_path),
                      ("references_csv", references_path)):
        if path:
            summary[key] = os.path.basename(path)
            summary[f"{key}_sha256"] = sha256(path)
    summary_path = os.path.join(out_dir, SUMMARY_NAME)
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=1)
        fh.write("\n")

    if inventory_out:
        # Written from the SAME in-memory counts as occurrence-summary.json, so
        # the two exports cannot disagree about what was measured; the summary
        # stays the raw record the inventory is compared against.
        try:
            labels, label_provenance = load_label_sources(label_paths)
            dataset = derive_dataset_label(release, tables)
            rows, columns = build_inventory(elements, all_rows, subjects,
                                            subject_basis, shapes, labels)
            inv_csv, inv_meta = write_inventory(
                rows, columns, inventory_out, dataset, {
                    "release": release,
                    "snapshot_digest": snapshot_digest(tables),
                    "warehouse": data_path,
                    "host": socket.gethostname(),
                    "extractor": {
                        "script": os.path.basename(__file__),
                        "sha256": sha256(os.path.abspath(__file__)),
                        "registry_sha256": sha256(DEFAULT_REGISTRY),
                    },
                    "tables": tables,
                    "label_sources": label_provenance,
                    "raw_summary": {
                        "file": SUMMARY_NAME,
                        "csv": CSV_NAME,
                        "csv_sha256": summary["csv_sha256"],
                    },
                    "versions": summary["versions"],
                    # A column whose subjects could not be measured has NULL
                    # subjects, which is indistinguishable in the CSV from an
                    # element that has no patient at all. Named here so the two
                    # are distinguishable somewhere.
                    "subject_measurement_errors": {
                        name: entry["subjects"]["error"]
                        for name, entry in per_element.items()
                        if isinstance(entry.get("subjects"), dict)
                        and "error" in entry["subjects"]
                    },
                })
            log(f"Wrote {inv_csv} ({len(rows):,} rows) and {inv_meta}")
            log(f"dataset label: {dataset}")
        except Exception as exc:  # noqa: BLE001 — the raw outputs above are
            # already on disk and are what a re-export needs; losing them to an
            # inventory error would cost another queue slot.
            err = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            log(f"INVENTORY EXPORT FAILED: {err}")
            failures += 1

    log(f"\nWrote {csv_path} ({len(all_rows):,} rows) and {summary_path}")
    for path, rows in ((shapes_path, shape_rows),
                       (references_path, reference_rows)):
        if path:
            log(f"Wrote {path} ({len(rows):,} rows)")
    if failures:
        log(f"{failures} element(s) failed — see 'elements' in the summary.")
    return 1 if failures else 0


def _pathling_version():
    try:
        from importlib.metadata import version
        return version("pathling")
    except Exception:  # noqa: BLE001
        return "unknown"


# --------------------------------------------------------------------------- #

def main(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", help="Pathling Delta warehouse root (one "
                                   "<ResourceType>.parquet table per resource)")
    ap.add_argument("--registry", default=DEFAULT_REGISTRY,
                    help="elements.json (default: next to this script)")
    ap.add_argument("--out-dir", default=SCRIPT_DIR,
                    help="where to write the CSV + summary (default: next to "
                         "this script)")
    ap.add_argument("--elements", nargs="+", metavar="ELEMENT",
                    help="only count these elements (partial run)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the planned views without importing Spark")
    ap.add_argument("--inventory-out", metavar="DIR",
                    help="also export the storage-space inventory "
                         "(<dataset>.csv + <dataset>.meta.json) here; requires "
                         "--release")
    ap.add_argument("--release", metavar="LABEL",
                    help="the data RELEASE this warehouse was loaded from, e.g. "
                         "mimic-iv-3.1. The dataset label is this plus a digest "
                         "of the observed Delta versions — a release label alone "
                         "does not identify a snapshot")
    ap.add_argument("--labels", nargs="+", metavar="PATH", default=[],
                    help="CodeSystem/ValueSet JSON files or directories whose "
                         "concept displays fill in missing warehouse labels. "
                         "Dictionary labels only — no ConceptMap is read")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    if args.inventory_out and not args.release:
        ap.error("--inventory-out requires --release")

    registry = load_registry(args.registry)
    elements = load_registry(args.registry, args.elements)
    log(f"Loaded {len(elements)} element(s) from {args.registry}")
    if args.dry_run:
        return do_dry_run(elements, registry)
    return do_run(elements, args.data, args.out_dir, registry,
                  inventory_out=args.inventory_out, release=args.release,
                  label_paths=args.labels)


if __name__ == "__main__":
    sys.exit(main())
