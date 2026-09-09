"""The storage-space inventory contract, without Spark.

The real extraction runs once, on a queue slot, against a warehouse no laptop
holds — so every property the committed artifact is trusted for is asserted here
over hand-built counts instead. All fixtures below are INVENTED: three-code
CodeSystems and single-digit patient counts, chosen to exercise a rule, never
copied or derived from MIMIC.
"""

import csv
import json

import pytest

import count_occurrences as co


# --------------------------------------------------------------------------- #
# Column keys
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("element,expected", [
    ("Condition.code", "Condition.code"),
    ("Observation.component.code", "Observation.component.code"),
    ("MedicationRequest.medication[x]", "MedicationRequest.medication"),
])
def test_scope_key_normalises_the_choice_marker(element, expected):
    assert co.scope_key({"element": element}) == expected


# --------------------------------------------------------------------------- #
# Snapshot identity
# --------------------------------------------------------------------------- #

TABLES = {
    "Condition": {"delta_version": 0, "committed": "2099-01-01 00:00:00", "rows": 4},
    "Medication": {"delta_version": 0, "committed": "2099-01-01 00:00:00", "rows": 2},
}


def test_snapshot_digest_tracks_the_observed_version_not_the_release():
    later = {**TABLES, "Condition": {**TABLES["Condition"], "delta_version": 1}}
    assert co.snapshot_digest(TABLES) != co.snapshot_digest(later)
    assert co.derive_dataset_label("mimic-iv-3.1", TABLES) \
        != co.derive_dataset_label("mimic-iv-3.1", later)


def test_snapshot_digest_refuses_an_unidentified_table():
    with pytest.raises(ValueError, match="Condition"):
        co.snapshot_digest({"Condition": {"error": "boom"}})


@pytest.mark.parametrize("release", ["", None, "../escape", "a/b", ".hidden",
                                     "x" * 200])
def test_unsafe_release_labels_are_rejected(release):
    with pytest.raises(ValueError):
        co.derive_dataset_label(release, TABLES)


def test_derived_label_is_itself_a_safe_filename():
    label = co.derive_dataset_label("mimic-iv-3.1", TABLES)
    assert co.re.fullmatch(co.SAFE_LABEL_RE, label)
    assert label.startswith("mimic-iv-3.1-")


# --------------------------------------------------------------------------- #
# Dictionary labels
# --------------------------------------------------------------------------- #

def _write(tmp_path, name, payload):
    path = tmp_path / name
    path.write_text(json.dumps(payload))
    return path


def test_labels_join_on_system_and_code_never_on_a_bare_code(tmp_path):
    _write(tmp_path, "CodeSystem-a.json", {
        "resourceType": "CodeSystem", "url": "http://a", "version": "1",
        "concept": [{"code": "1", "display": "alpha one"}],
    })
    _write(tmp_path, "CodeSystem-b.json", {
        "resourceType": "CodeSystem", "url": "http://b", "version": "1",
        "concept": [{"code": "1", "display": "beta one"}],
    })
    labels, provenance = co.load_label_sources([str(tmp_path)])

    # The same bare code in two systems must not collapse.
    assert labels[("http://a", "1")] == "alpha one"
    assert labels[("http://b", "1")] == "beta one"
    assert [p["url"] for p in provenance] == ["http://a", "http://b"]
    # Provenance is a content digest, because the mapping outputs in this repo
    # are regenerated in the working tree and a revision does not identify them.
    assert all(len(p["sha256"]) == 64 for p in provenance)


def test_labels_read_a_valueset_expansion_and_a_nested_codesystem(tmp_path):
    path = _write(tmp_path, "vs.json", {
        "resourceType": "ValueSet", "url": "http://vs",
        "expansion": {"contains": [
            {"system": "http://a", "code": "2", "display": "alpha two"}]},
    })
    nested = _write(tmp_path, "cs.json", {
        "resourceType": "CodeSystem", "url": "http://c",
        "concept": [{"code": "p", "display": "parent",
                     "concept": [{"code": "k", "display": "kid"}]}],
    })
    labels, _ = co.load_label_sources([str(path), str(nested)])
    assert labels[("http://a", "2")] == "alpha two"
    assert labels[("http://c", "k")] == "kid"


def test_no_conceptmap_is_ever_read(tmp_path):
    """Classification is the consumer's job; the generator must not do it."""
    path = _write(tmp_path, "cm.json", {
        "resourceType": "ConceptMap", "url": "http://cm",
        "group": [{"source": "http://a", "target": "http://sct", "element": [
            {"code": "1", "target": [{"code": "99", "display": "mapped",
                                      "equivalence": "equivalent"}]}]}],
    })
    labels, provenance = co.load_label_sources([str(path)])
    assert labels == {}
    assert provenance[0]["resource_type"] == "ConceptMap"
    assert provenance[0]["labels"] == 0


# --------------------------------------------------------------------------- #
# The inventory itself
# --------------------------------------------------------------------------- #

CONDITION = {"element": "Condition.code", "resource_type": "Condition",
             "extract_path": "code.coding",
             "coded_exists": "code.coding.exists()", "subject_path": "subject"}
COMPONENT = {"element": "Observation.component.code",
             "resource_type": "Observation",
             "extract_path": "component.code.coding",
             "coded_exists": "component.code.coding.exists()",
             "subject_path": "subject"}
MEDICATION = {"element": "Medication.code", "resource_type": "Medication",
              "extract_path": "code.coding",
              "coded_exists": "code.coding.exists()",
              "occurrence_kind": "dictionary"}


def _row(element, system, code, display, occurrences):
    return {"element": element, "system": system, "code": code,
            "display": display, "occurrences": occurrences}


def test_multi_coding_resource_reports_more_codings_than_resources():
    """Three resources, two codings each — the old universal 1:1 assumption."""
    rows, columns = co.build_inventory(
        [CONDITION],
        [_row("Condition.code", "http://icd9", "51882", "ARDS, icd9", 3),
         _row("Condition.code", "http://icd10", "J80", "ARDS, icd10", 3)],
        subjects={}, subject_basis={},
        shapes={"Condition.code": {"total_resources": 3,
                                   "uncoded_resources": 0}},
        labels={})
    entry = columns["Condition.code"]
    assert entry["coding_occurrences"] == 6 > entry["total_resources"] == 3
    assert entry["codes"] == 2
    assert len(rows) == 2


def test_repeating_component_uncoded_count_is_declared_not_subtracted():
    """Subtracting codings from resources would be negative here, not just wrong."""
    rows, columns = co.build_inventory(
        [COMPONENT],
        [_row("Observation.component.code", "http://loinc", "8480-6", "sbp", 40)],
        subjects={}, subject_basis={},
        # 10 Observations, 40 component codings, 3 with no component coding.
        shapes={"Observation.component.code": {"total_resources": 10,
                                               "uncoded_resources": 3}},
        labels={})
    entry = columns["Observation.component.code"]
    assert entry["uncoded_resources"] == 3
    assert entry["total_resources"] - entry["coding_occurrences"] < 0
    assert len(rows) == 1


def test_element_with_no_codings_at_all_still_exports_its_denominator():
    rows, columns = co.build_inventory(
        [CONDITION], [], subjects={}, subject_basis={},
        shapes={"Condition.code": {"total_resources": 7,
                                   "uncoded_resources": 7}},
        labels={})
    assert rows == []
    assert columns["Condition.code"] == {
        "codes": 0, "coding_occurrences": 0,
        "invalid_identity_coding_occurrences": 0,
        "total_resources": 7, "uncoded_resources": 7,
        "occurrence_kind": co.EVENT_CODING, "subject_basis": None,
    }


def test_dictionary_resource_is_labelled_and_has_no_patient_count():
    rows, columns = co.build_inventory(
        [MEDICATION],
        [_row("Medication.code", "http://ndc", "0001", "", 1)],
        subjects={}, subject_basis={},
        shapes={"Medication.code": {"total_resources": 1,
                                    "uncoded_resources": 0}},
        labels={})
    assert columns["Medication.code"]["occurrence_kind"] == co.DICTIONARY_CODING
    assert columns["Medication.code"]["subject_basis"] is None
    assert rows[0]["subjects"] is None


def test_missing_display_is_enriched_by_label_and_otherwise_left_empty():
    rows, _ = co.build_inventory(
        [CONDITION],
        [_row("Condition.code", "http://icd9", "51882", "", 2),
         _row("Condition.code", "http://icd9", "99999", "", 1),
         _row("Condition.code", "http://icd10", "J80", "warehouse label", 1)],
        subjects={}, subject_basis={},
        shapes={"Condition.code": {"total_resources": 4,
                                   "uncoded_resources": 0}},
        labels={("http://icd9", "51882"): "ARDS from the dictionary"})
    display = {r["stored_code"]: r["stored_display"] for r in rows}
    assert display["51882"] == "ARDS from the dictionary"
    # Unlabelled: still exported, still searchable by code, never invented.
    assert display["99999"] == ""
    # A trustworthy warehouse display wins over the dictionary.
    assert display["J80"] == "warehouse label"


def test_invalid_identities_are_excluded_from_the_csv_but_reconcile():
    rows, columns = co.build_inventory(
        [CONDITION],
        [_row("Condition.code", "http://icd9", "51882", "ARDS", 5),
         _row("Condition.code", "http://icd9", "", "text only", 2),
         _row("Condition.code", "", "orphan", "no system", 1)],
        subjects={}, subject_basis={},
        shapes={"Condition.code": {"total_resources": 8,
                                   "uncoded_resources": 0}},
        labels={})
    entry = columns["Condition.code"]
    assert [r["stored_code"] for r in rows] == ["51882"]
    assert entry["invalid_identity_coding_occurrences"] == 3
    assert sum(r["occurrences"] for r in rows) \
        + entry["invalid_identity_coding_occurrences"] \
        == entry["coding_occurrences"]


def test_a_column_without_a_resource_denominator_blocks_the_artifact():
    with pytest.raises(ValueError, match="Condition.code"):
        co.build_inventory(
            [CONDITION],
            [_row("Condition.code", "http://icd9", "51882", "ARDS", 1)],
            subjects={}, subject_basis={}, shapes={}, labels={})


@pytest.mark.parametrize("shape", [
    {"total_resources": 3},
    {"total_resources": 3, "uncoded_resources": None},
    {"total_resources": None, "uncoded_resources": 0},
])
def test_a_partial_resource_measurement_is_not_a_partial_artifact(shape):
    with pytest.raises(ValueError):
        co.build_inventory(
            [CONDITION], [], subjects={}, subject_basis={},
            shapes={"Condition.code": shape}, labels={})


# --------------------------------------------------------------------------- #
# Patient counts
# --------------------------------------------------------------------------- #

def test_exact_and_incomplete_patient_association():
    """An identity with any subject-less coding gets null, not a lower bound."""
    rows, columns = co.build_inventory(
        [CONDITION],
        [_row("Condition.code", "http://icd9", "51882", "ARDS", 5),
         _row("Condition.code", "http://icd10", "J80", "ARDS", 4)],
        subjects={"Condition.code": {
            ("http://icd9", "51882"): 3,
            # Measured over 4 codings of which one had no resolvable subject.
            ("http://icd10", "J80"): None,
        }},
        subject_basis={"Condition.code": "subject.getReferenceKey()"},
        shapes={"Condition.code": {"total_resources": 9,
                                   "uncoded_resources": 0}},
        labels={})
    subjects = {r["stored_code"]: r["subjects"] for r in rows}
    assert subjects == {"51882": 3, "J80": None}
    assert columns["Condition.code"]["subject_basis"] == "subject.getReferenceKey()"


# --------------------------------------------------------------------------- #
# Serialisation
# --------------------------------------------------------------------------- #

def _built(tmp_path, **kwargs):
    rows, columns = co.build_inventory(
        [CONDITION],
        [_row("Condition.code", "http://icd9", "51882", "ARDS", 5),
         _row("Condition.code", "http://icd10", "J80", "ARDS", 4)],
        subjects={"Condition.code": {("http://icd9", "51882"): 3,
                                     ("http://icd10", "J80"): None}},
        subject_basis={"Condition.code": "subject.getReferenceKey()"},
        shapes={"Condition.code": {"total_resources": 9,
                                   "uncoded_resources": 0}},
        labels={})
    return co.write_inventory(rows, columns, str(tmp_path), "synthetic-v1",
                              kwargs)


def test_unavailable_subjects_are_written_empty_never_zero(tmp_path):
    csv_path, meta_path = _built(tmp_path)
    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == co.INVENTORY_COLUMNS
        written = {r["stored_code"]: r["subjects"] for r in reader}
    assert written == {"51882": "3", "J80": ""}

    manifest = json.loads(open(meta_path).read())
    assert manifest["schema_version"] == co.INVENTORY_SCHEMA_VERSION
    assert manifest["dataset"] == "synthetic-v1"
    assert manifest["csv"] == "synthetic-v1.csv"
    assert manifest["rows"] == 2
    assert "columns" in manifest and "generated_at" in manifest


def test_the_manifest_digest_detects_a_truncated_csv(tmp_path):
    csv_path, meta_path = _built(tmp_path)
    recorded = json.loads(open(meta_path).read())["csv_sha256"]
    assert recorded == co.sha256(csv_path)
    with open(csv_path, "a") as fh:
        fh.write("Condition.code,http://icd9,0000,injected,1,\n")
    assert recorded != co.sha256(csv_path)


def test_csv_ordering_is_fully_determined_by_the_data(tmp_path):
    first, _ = _built(tmp_path)
    body = open(first).read()
    second, _ = _built(tmp_path / "again")
    assert open(second).read() == body


def test_extra_provenance_is_carried_into_the_manifest(tmp_path):
    _, meta_path = _built(tmp_path, release="mimic-iv-3.1",
                          snapshot_digest="deadbeef")
    manifest = json.loads(open(meta_path).read())
    assert manifest["release"] == "mimic-iv-3.1"
    assert manifest["snapshot_digest"] == "deadbeef"
    # Stated in the artifact, not only in a plan: a digest is file identity.
    assert "identity_limitation" in manifest


def test_a_duplicate_identity_is_refused(tmp_path):
    row = {"column": "Condition.code", "stored_system": "http://icd9",
           "stored_code": "51882", "stored_display": "ARDS",
           "occurrences": 1, "subjects": None}
    with pytest.raises(ValueError, match="duplicate"):
        co.write_inventory([row, dict(row)], {}, str(tmp_path), "dup-v1", {})


# --------------------------------------------------------------------------- #
# The committed registry must actually support the export
# --------------------------------------------------------------------------- #

def test_every_registered_element_declares_what_the_inventory_needs():
    with open(co.DEFAULT_REGISTRY) as fh:
        elements = json.load(fh)["elements"]
    for element in elements:
        assert element["coded_exists"].endswith(".exists()"), element["element"]
        # A dictionary element has no patient, and inventing a referring
        # population for it is exactly what v1 refuses to do.
        if element.get("occurrence_kind") == "dictionary":
            assert "subject_path" not in element
        else:
            assert element["subject_path"] == "subject", element["element"]
    assert len({co.scope_key(e) for e in elements}) == len(elements)
