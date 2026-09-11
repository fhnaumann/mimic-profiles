"""Unit Quantity extraction and coverage calculation, without Spark."""

import csv
import hashlib
import json

import build_unit_coverage as coverage
import count_unit_occurrences as extraction


def test_quantity_plan_reads_direct_system_code_and_unit():
    plan = extraction.plan_view(extraction.LOCATIONS[0])
    select = plan["select"][0]
    assert select["forEach"] == "value.ofType(Quantity)"
    assert [column["path"] for column in select["column"]] == [
        "system", "code", "unit"]


def test_coverage_uses_observed_enumerated_codes_and_validates_hash(tmp_path):
    table = tmp_path / "units.csv"
    table.write_text(
        "mimic_code,mimic_display,ucum_code,ucum_display\n"
        "mg,mg,mg,mg\n"
        "bad,bad,,\n"
        "unused,unused,mL,mL\n")
    occurrence_file = tmp_path / "unit-occurrences.csv"
    with occurrence_file.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=extraction.CSV_COLUMNS,
                                lineterminator="\n")
        writer.writeheader()
        writer.writerows([
            {"location": "Observation.value[x]", "resource_type": "Observation",
             "system": coverage.MIMIC_UNITS, "code": "mg", "unit": "mg",
             "occurrences": 90},
            {"location": "Observation.value[x]", "resource_type": "Observation",
             "system": coverage.MIMIC_UNITS, "code": "bad", "unit": "bad",
             "occurrences": 10},
            {"location": "Observation.value[x]", "resource_type": "Observation",
             "system": coverage.UCUM, "code": "mm[Hg]", "unit": "mmHg",
             "occurrences": 5},
        ])
    locations = {name: {"path": "fixture", "quantities": 0}
                 for name in coverage.EXPECTED_LOCATIONS}
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({
        "failures": 0,
        "csv_sha256": hashlib.sha256(occurrence_file.read_bytes()).hexdigest(),
        "locations": locations,
        "warehouse": "fixture",
        "generated": "2099-01-01T00:00:00Z",
        "tables": {},
        "scope": "fixture",
    }))

    result = coverage.build(table, occurrence_file, summary)
    assert result["units_stream"] == {
        "enumerated_total": 3,
        "mapped_enumerated": 2,
        "observed_total": 2,
        "mapped_observed": 1,
        "code_coverage_pct": 50.0,
        "occurrences_total": 100,
        "occurrences_mapped": 90,
        "occurrence_coverage_pct": 90.0,
    }
    assert result["ucum_native_identity"]["occurrences_total"] == 5
