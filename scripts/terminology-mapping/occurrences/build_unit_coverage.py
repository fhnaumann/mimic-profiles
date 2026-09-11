#!/usr/bin/env python3
"""Validate unit-occurrences.csv and calculate UCUM stream coverage."""

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

MIMIC_UNITS = "http://mimic.mit.edu/fhir/mimic/CodeSystem/mimic-units"
UCUM = "http://unitsofmeasure.org"
EXPECTED_LOCATIONS = {
    "Observation.value[x]",
    "Observation.component.value[x]",
    "MedicationAdministration.dosage.dose",
    "MedicationAdministration.dosage.rate[x]",
    "MedicationRequest.dosageInstruction.doseAndRate.dose[x]",
    "MedicationRequest.dosageInstruction.doseAndRate.rate[x]",
    "MedicationDispense.dosageInstruction.doseAndRate.dose[x]",
    "MedicationDispense.dosageInstruction.doseAndRate.rate[x]",
}


def floor_pct(numerator, denominator):
    return math.floor(10000 * numerator / denominator) / 100 if denominator else 0.0


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(table_path, csv_path, summary_path):
    summary = json.loads(summary_path.read_text())
    if summary.get("failures") != 0:
        raise ValueError(f"extraction has {summary.get('failures')} failure(s)")
    if sha256(csv_path) != summary.get("csv_sha256"):
        raise ValueError("unit occurrence CSV sha256 does not match its summary")
    actual_locations = set(summary.get("locations", {}))
    if actual_locations != EXPECTED_LOCATIONS:
        raise ValueError(f"location scope mismatch: {sorted(actual_locations ^ EXPECTED_LOCATIONS)}")
    errors = {name: value["error"] for name, value in summary["locations"].items()
              if "error" in value}
    if errors:
        raise ValueError(f"location extraction errors: {errors}")

    table_rows = list(csv.DictReader(table_path.open()))
    enumeration = {row["mimic_code"] for row in table_rows}
    mapped = {row["mimic_code"] for row in table_rows if row["ucum_code"]}
    if len(enumeration) != len(table_rows):
        raise ValueError("duplicate mimic_code in units mapping table")

    occurrences = Counter()
    native = Counter()
    invalid = Counter()
    by_location = defaultdict(lambda: Counter(total=0, mapped=0))
    for row in csv.DictReader(csv_path.open()):
        count = int(row["occurrences"])
        key = (row["system"], row["code"])
        if row["system"] == MIMIC_UNITS and row["code"] in enumeration:
            occurrences[row["code"]] += count
            by_location[row["location"]]["total"] += count
            if row["code"] in mapped:
                by_location[row["location"]]["mapped"] += count
        elif row["system"] == UCUM and row["code"]:
            native[row["code"]] += count
        else:
            invalid[key] += count

    observed = set(occurrences)
    mapped_observed = observed & mapped
    occurrence_total = sum(occurrences.values())
    occurrence_mapped = sum(occurrences[code] for code in mapped_observed)
    result = {
        "source_artifact": str(csv_path),
        "source_sha256": summary["csv_sha256"],
        "mapping_table": str(table_path),
        "mapping_table_sha256": sha256(table_path),
        "warehouse": summary["warehouse"],
        "generated": summary["generated"],
        "tables": summary["tables"],
        "scope": summary["scope"],
        "units_stream": {
            "enumerated_total": len(enumeration),
            "mapped_enumerated": len(mapped),
            "observed_total": len(observed),
            "mapped_observed": len(mapped_observed),
            "code_coverage_pct": floor_pct(len(mapped_observed), len(observed)),
            "occurrences_total": occurrence_total,
            "occurrences_mapped": occurrence_mapped,
            "occurrence_coverage_pct": floor_pct(occurrence_mapped, occurrence_total),
        },
        "ucum_native_identity": {
            "observed_total": len(native),
            "mapped_observed": len(native),
            "occurrences_total": sum(native.values()),
            "occurrences_mapped": sum(native.values()),
            "code_coverage_pct": 100.0,
            "occurrence_coverage_pct": 100.0,
        },
        "excluded_invalid_or_unenumerated": {
            "identities": len(invalid),
            "occurrences": sum(invalid.values()),
            "by_identity": [{"system": system, "code": code, "occurrences": count}
                            for (system, code), count in sorted(
                                invalid.items(), key=lambda item: -item[1])],
        },
        "by_location": {
            name: {**counts,
                   "occurrence_coverage_pct": floor_pct(counts["mapped"], counts["total"])}
            for name, counts in sorted(by_location.items())
        },
    }
    return result


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", type=Path, default=root / "conceptmaps/units-ucum.csv")
    parser.add_argument("--occurrences", type=Path,
                        default=Path(__file__).parent / "unit-output/unit-occurrences.csv")
    parser.add_argument("--summary", type=Path,
                        default=Path(__file__).parent / "unit-output/unit-occurrence-summary.json")
    parser.add_argument("--out", type=Path,
                        default=Path(__file__).parent / "unit-output/unit-coverage.json")
    args = parser.parse_args()
    result = build(args.table, args.occurrences, args.summary)
    args.out.write_text(json.dumps(result, indent=1) + "\n")
    values = result["units_stream"]
    print(f"units: {values['mapped_observed']}/{values['observed_total']} codes "
          f"({values['code_coverage_pct']:.2f}%), "
          f"{values['occurrences_mapped']}/{values['occurrences_total']} occurrences "
          f"({values['occurrence_coverage_pct']:.2f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
