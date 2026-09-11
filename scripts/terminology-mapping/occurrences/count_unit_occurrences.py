#!/usr/bin/env python3
"""Count Quantity.system/code occurrences served by mimic-units-to-ucum.

This is a unit-specific companion to count_occurrences.py. Quantity is a
primitive-coded datatype (system/code/unit directly), not CodeableConcept, and
it occurs at several paths across four resource tables. Keeping this extraction
separate avoids rewriting the committed full-registry occurrence snapshot.

The paths are the Quantity locations populated by the MIMIC ETL and served by
the map: Observation value/component values and medication dosage dose/rate
quantities. Outputs are unit-occurrences.csv and unit-occurrence-summary.json.
"""

import argparse
import csv
import hashlib
import importlib.metadata
import json
import os
import platform
import socket
import sys
import traceback
from datetime import datetime, timezone

CSV_NAME = "unit-occurrences.csv"
SUMMARY_NAME = "unit-occurrence-summary.json"
CSV_COLUMNS = ["location", "resource_type", "system", "code", "unit", "occurrences"]

# These paths come from the IG profiles/examples and mapping documentation under
# input/includes/. Choice fields use their FHIRPath names, not JSON suffixes.
LOCATIONS = [
    {"location": "Observation.value[x]", "resource_type": "Observation",
     "path": "value.ofType(Quantity)"},
    {"location": "Observation.component.value[x]", "resource_type": "Observation",
     "path": "component.value.ofType(Quantity)"},
    {"location": "MedicationAdministration.dosage.dose", "resource_type": "MedicationAdministration",
     "path": "dosage.dose"},
    {"location": "MedicationAdministration.dosage.rate[x]", "resource_type": "MedicationAdministration",
     "path": "dosage.rate.ofType(Quantity)"},
    {"location": "MedicationRequest.dosageInstruction.doseAndRate.dose[x]", "resource_type": "MedicationRequest",
     "path": "dosageInstruction.doseAndRate.dose.ofType(Quantity)"},
    {"location": "MedicationRequest.dosageInstruction.doseAndRate.rate[x]", "resource_type": "MedicationRequest",
     "path": "dosageInstruction.doseAndRate.rate.ofType(Quantity)"},
    {"location": "MedicationDispense.dosageInstruction.doseAndRate.dose[x]", "resource_type": "MedicationDispense",
     "path": "dosageInstruction.doseAndRate.dose.ofType(Quantity)"},
    {"location": "MedicationDispense.dosageInstruction.doseAndRate.rate[x]", "resource_type": "MedicationDispense",
     "path": "dosageInstruction.doseAndRate.rate.ofType(Quantity)"},
]


def log(message):
    print(message, file=sys.stderr, flush=True)


def plan_view(location):
    return {
        "resource_type": location["resource_type"],
        "select": [{
            "forEach": location["path"],
            "column": [
                {"path": "system", "name": "system"},
                {"path": "code", "name": "code"},
                {"path": "unit", "name": "unit"},
            ],
        }],
    }


def sha256(path):
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


def delta_identity(spark, data_path, resource_type):
    table = f"{data_path.rstrip('/')}/{resource_type}.parquet"
    try:
        history = (spark.sql(f"DESCRIBE HISTORY delta.`{table}`")
                        .select("version", "timestamp")
                        .orderBy("version", ascending=False).first())
        rows = spark.sql(f"SELECT count(*) AS n FROM delta.`{table}`").first()["n"]
        return {"delta_version": history["version"],
                "committed": str(history["timestamp"]), "rows": rows}
    except Exception as exc:  # provenance failure must remain visible
        return {"error": "".join(
            traceback.format_exception_only(type(exc), exc)).strip()}


def count_location(data, location):
    from pyspark.sql import functions as functions

    plan = plan_view(location)
    frame = data.view(plan["resource_type"], select=plan["select"])
    aggregate = (frame.groupBy("system", "code", "unit")
                      .agg(functions.count(functions.lit(1)).alias("occurrences"))
                      .orderBy(functions.desc("occurrences"), "system", "code", "unit"))
    rows = [{"location": location["location"],
             "resource_type": location["resource_type"],
             "system": row["system"] or "", "code": row["code"] or "",
             "unit": row["unit"] or "", "occurrences": row["occurrences"]}
            for row in aggregate.collect()]
    return rows, {
        "quantities": sum(row["occurrences"] for row in rows),
        "distinct_identities": len({(row["system"], row["code"]) for row in rows}),
        "without_system": sum(row["occurrences"] for row in rows if not row["system"]),
        "without_code": sum(row["occurrences"] for row in rows if not row["code"]),
    }


def write_outputs(rows, summary, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, CSV_NAME)
    rows.sort(key=lambda row: (row["location"], -row["occurrences"],
                               row["system"], row["code"], row["unit"]))
    with open(csv_path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    summary["csv"] = CSV_NAME
    summary["csv_sha256"] = sha256(csv_path)
    summary_path = os.path.join(out_dir, SUMMARY_NAME)
    with open(summary_path, "w") as handle:
        json.dump(summary, handle, indent=1)
        handle.write("\n")
    return csv_path, summary_path


def dry_run():
    for location in LOCATIONS:
        plan = plan_view(location)
        print(f"{location['location']}: {plan['resource_type']} -> {location['path']}")
        print(json.dumps(plan["select"]))
    return 0


def run(data_path, out_dir):
    from pathling import PathlingContext
    import pyspark

    context = PathlingContext.create()
    data = context.read.delta(data_path)
    all_rows = []
    per_location = {}
    tables = {}
    failures = 0
    for index, location in enumerate(LOCATIONS, 1):
        resource_type = location["resource_type"]
        if resource_type not in tables:
            tables[resource_type] = delta_identity(context.spark, data_path, resource_type)
        try:
            rows, totals = count_location(data, location)
            all_rows.extend(rows)
            per_location[location["location"]] = {"path": location["path"], **totals}
            log(f"[{index}/{len(LOCATIONS)}] {location['location']}: "
                f"{totals['quantities']:,} quantities, "
                f"{totals['distinct_identities']:,} identities")
        except Exception as exc:
            error = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            per_location[location["location"]] = {"path": location["path"], "error": error}
            failures += 1
            log(f"[{index}/{len(LOCATIONS)}] {location['location']}: ERROR {error}")

    summary = {
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": socket.gethostname(), "warehouse": data_path,
        "scope": "Quantity.code at Observation value/component and medication dosage dose/rate Quantity paths",
        "versions": {"python": platform.python_version(),
                     "spark": pyspark.__version__,
                     "pathling": importlib.metadata.version("pathling")},
        "tables": tables, "locations": per_location, "failures": failures,
    }
    csv_path, summary_path = write_outputs(all_rows, summary, out_dir)
    log(f"wrote {csv_path} and {summary_path}")
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data")
    parser.add_argument("--out-dir", default="occurrences/unit-output")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.dry_run:
        return dry_run()
    if not args.data:
        parser.error("--data is required unless --dry-run is used")
    return run(args.data, args.out_dir)


if __name__ == "__main__":
    sys.exit(main())
