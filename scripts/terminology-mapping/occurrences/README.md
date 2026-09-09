# Occurrence counts

How often every coded value actually occurs in the full MIMIC-on-FHIR
warehouse, so the mapping coverage tables can be weighted by data volume.
Coverage over *codes* answers "how much of the dictionary did we map"; coverage
over *occurrences* answers "how likely is a data point I meet to carry an
unmapped code", and the gap between the two is the interesting part — 80% of
codes mapped but 50% of rows means the frequent codes are the unmapped ones.

Committed artefacts, produced by one HPC run and then never regenerated until
the warehouse itself changes:

| File | What |
|---|---|
| `code-occurrences.csv` | `element,system,code,display,occurrences` — every distinct coded value, with its row count |
| `element-shapes.csv` | `element,shape,resources` — for a CHOICE element, how many resources took each branch. Only for elements declaring `count_shapes` |
| `reference-occurrences.csv` | `element,via,system,code,display,resources` — codes reached by following a Reference, counted once per *referring* resource. Only for elements declaring `reference_join` |
| `occurrence-summary.json` | per-element totals plus the run's identity: Delta version + commit timestamp per table, host, versions, and the sha256 of each CSV |
| `elements.json` | the registry both sides read: resource type + FHIRPath for the node, bound ValueSets for the local classification |

`build_statistics.py` verifies the CSV against `csv_sha256` before using it, so
a half-copied or hand-edited file cannot quietly enter a thesis table. Without
these files present, `make statistics` behaves exactly as it did before them.

## A coding count is not a resource count

`code-occurrences.csv` counts **codings**, and on a choice element that is not
the same population as the resources. `MedicationRequest.medication[x]` is
`CodeableConcept | Reference(Medication)`, and only the CodeableConcept branch
carries a Coding — so the 2026-08-06 run recorded 1,883,681 codings against
15,416,901 MedicationRequest rows. At least 87.8% of prescriptions carry their
drug identity on `Medication.code`, reached through the reference, and a
coverage percentage over the codings alone reads as a statement about
prescriptions when it is a statement about one branch of one element.

That is what the two side files exist for, and why `Medication.code` is in the
registry at all. `element-shapes.csv` says how the choice was actually taken;
`reference-occurrences.csv` follows the reference so resource-level reachability
can be computed. Note also that a `required` binding on a choice element **cannot
constrain the Reference branch** — a validator has nothing coded to check — so
for most MIMIC prescriptions the terminology guarantee is carried entirely by
`Medication.code`'s own binding.

A useful by-product: `with_codeable_concept` true together with `with_coding`
false is a CodeableConcept carrying `text` and no `Coding`. Under a required
binding on the whole CodeableConcept that is non-conformant, and the summary
reports it as `codeable_concept_without_coding`. On
`MedicationStatement.medication[x]` it is expected and legal, because that
profile binds the coding *slices* rather than the CodeableConcept.

## Not every element's count is data volume

`Medication.code` is in the registry and is counted, but its counts are the
**size of the drug dictionary**, not data volume. MIMIC mints one `Medication`
per distinct `(drug, gsn, ndc, formulary_drug_cd)` tuple — `drug_code` converted
to UUID5, see `input/includes/map-medicationrequest.md` — so a code used on ten
thousand prescriptions still occurs about once there. It is counted anyway
because `observed_only` needs to know which codes appear *at all*, and because
dictionary reachability is worth reporting.

`occurrence_kind` marks the distinction and `build_statistics.py` reports the two
in separate tables. The volume question for a dictionary element is answered by
`reference-occurrences.csv`, which counts its codes once per *referring*
resource — that figure is comparable with the event elements, and the dictionary
count is not.

Related: do not sum `occurrences` across elements. The same drug-name code is
counted on `MedicationRequest.medication[x]`, `MedicationDispense.medication[x]`,
`MedicationAdministration.medication[x]` and `Medication.code`, because each is a
separate binding with its own map. Per-element is the only level at which the
numbers mean anything.

## Re-running it on the node

Cluster conventions (account, modules, no internet on compute nodes, no
polling) come from the `csiro-hpc` and `hpc-transfer` skills — defer to those
for anything not stated here.

```bash
# 1. Dry run locally first: prints the planned views, imports no Spark.
uv run scripts/terminology-mapping/occurrences/count_occurrences.py --dry-run

# 2. Stage to scratch3 (script + registry + slurm only; outputs come back later).
ssh nau025@petrichor.hpc.csiro.au 'mkdir -p /scratch3/nau025/mimic-on-fhir-delta/occurrences'
rsync -av --exclude __pycache__ --exclude 'code-occurrences.csv' \
  --exclude 'occurrence-summary.json' --exclude 'element-shapes.csv' \
  --exclude 'reference-occurrences.csv' --exclude README.md \
  scripts/terminology-mapping/occurrences/ \
  nau025@petrichor.hpc.csiro.au:/scratch3/nau025/mimic-on-fhir-delta/occurrences/

# 3. Smoke test on the LOGIN node — cheap, and catches env drift before a job
#    burns a queue slot. Do not create a PathlingContext here (heavy JVM).
ssh nau025@petrichor.hpc.csiro.au 'bash -lc "
  module load python/3.12.3 amazon-corretto/21.0.0.35.1 &&
  cd /scratch3/nau025/mimic-on-fhir-delta &&
  test -d spark_warehouse && echo warehouse OK &&
  uv run python3 -c \"from pathling import PathlingContext; print(0)\" &&
  uv run python3 occurrences/count_occurrences.py --dry-run >/dev/null &&
  echo dry-run OK
"'

# 4. Submit. Status arrives by email (BEGIN/END/FAIL) — do not poll.
ssh nau025@petrichor.hpc.csiro.au \
  'cd /scratch3/nau025/mimic-on-fhir-delta/occurrences && sbatch count_occurrences.slurm'

# 5. Fetch back (no --delete: never remove committed artefacts).
rsync -av \
  nau025@petrichor.hpc.csiro.au:/scratch3/nau025/mimic-on-fhir-delta/occurrences/code-occurrences.csv \
  nau025@petrichor.hpc.csiro.au:/scratch3/nau025/mimic-on-fhir-delta/occurrences/element-shapes.csv \
  nau025@petrichor.hpc.csiro.au:/scratch3/nau025/mimic-on-fhir-delta/occurrences/reference-occurrences.csv \
  nau025@petrichor.hpc.csiro.au:/scratch3/nau025/mimic-on-fhir-delta/occurrences/occurrence-summary.json \
  scripts/terminology-mapping/occurrences/

# 6. Then, locally:
make statistics
```

The job is **read-only** — it never writes to the warehouse. That is the
difference from `merged_profile_provisioning`, which does.

## Why not the artefacts that already exist

`scripts/binding-analysis/distinct-codes.ndjson` already has counts in its `n`
column, and this script's query shape is lifted from
`phase1_extract_distinct.py`. But phase 0 filtered its work items to elements
that were *not* already bound, which excludes the three elements this repo maps
most: `Observation.code`, `Procedure.code` and `Specimen.type`. Topping that
file up would leave the statistics citing two extraction runs against warehouse
states separated by a provisioning step, so all ten elements are recounted
here in one job with one provenance record. `distinct-codes.ndjson` stays what
it is: evidence for the July binding analysis.

`pathling_mcp_tool`'s `get_cardinality_and_top_values_impl` was the other
candidate. It caps results at 20 values, which is useless for a field whose
whole story is its long tail, and it explodes several array columns
independently — cross-joining the codings of a multi-coding CodeableConcept
rather than keeping them aligned. The SQL-on-FHIR `forEach` used here gets that
right by construction.

## What is deliberately not in the artifact

**No `meta.profile` column.** A resource may carry more than one profile, so
grouping on an exploded `meta.profile` would count its codings once per
profile. Population identity survives without it: each unbuilt population has
its own CodeSystem (`mimic-chartevents-d-items`, `mimic-d-labitems`,
`mimic-microbiology-organism`), so a code's system already says which
population it came from.

**No patient or encounter counts in *these* files.** For the coverage metric —
"how likely is a data point to carry an unmapped code" — the answer is rows by
definition and a patient count never enters it. A nullable, exact
distinct-patient count *is* exported, but only in the storage-space inventory
below, which answers a different question for a different consumer. No patient
identifier and no patient membership set is written by any of these outputs.

---

## The storage-space inventory (`--inventory-out`)

A **second export of the same counts**, for a different consumer: the
cohort-selection orchestrator's code-pool add surface, which lets a researcher
add a code that terminology search missed. It needs to know which codes the
warehouse actually stores, how much data each carries, and nothing else.

```
<dataset>.csv        column,stored_system,stored_code,stored_display,occurrences,subjects
<dataset>.meta.json  per-column count grain + resource-grain totals + provenance
```

It is a re-export, not a second extraction: `build_inventory` is pure over the
counts already collected in memory, so the two artifacts cannot disagree about
what was measured, and `occurrence-summary.json` stays the raw record the
inventory is checked against.

### What the columns mean, and what they do not

- `column` is **resource-qualified**, with the choice marker dropped —
  `MedicationRequest.medication`, not `medication[x]` and not a table name.
- `occurrences` counts **coding occurrences**, including repeated codings and
  repeated components. It is not resources, events, or patients. For
  `Medication.code` it is dictionary size, and `occurrence_kind` in the manifest
  says so per column (`event_coding` / `dictionary_coding`) so the two can never
  be compared by accident.
- `subjects` is an **exact distinct-patient count or empty**. Never zero for
  "unmeasured", never a lower bound: any stored identity with even one coding
  whose subject reference does not resolve gets an empty field, and every
  dictionary element gets one for every row, because `Medication` has no patient
  and choosing a referring population for it would be inventing one.
  Reference-weighted patient counts are deliberately not attempted.
- `uncoded_resources` is **measured**, via each element's declared
  `coded_exists` FHIRPath, not derived as `total_resources - occurrences`. That
  subtraction is negative for `Observation.component.code` before it is wrong in
  any interesting way.
- Codings with no system or no code are **invalid identities**: excluded from the
  CSV, because the consumer keys on `(column, system, code)` and cannot address
  them, but counted in the manifest, so
  `sum(occurrences) + invalid_identity_coding_occurrences` reconciles with the
  column's coding total instead of the denominator quietly shrinking.

### No classification

The inventory is storage-space only. There are **no target codes and no
reachability classification**, and `--labels` reads CodeSystem/ValueSet
*displays* only — never a ConceptMap. Deciding whether a stored code is
reachable requires the consumer's translation policy, and a second copy of that
policy here would be a copy that silently diverges. The consumer classifies.

### Snapshot identity is derived, not typed

`--release` names the data release. The dataset label appended to it is a digest
of the **observed Delta version and commit timestamp of every table read**, and
the script prints it.

This is not pedantry. `mimic-iv-3.1` named the warehouse counted on 2026-08-06
(Delta versions 2 and 3) *and* the one that replaced it on 2026-08-24 (every
table rewritten to a single version 0). The release label would have read as
unchanged across a reload that invalidated the entire committed artifact — which
is what actually happened. `csv_sha256` proves file identity and `dataset` proves
which snapshot was counted; **neither proves the live server still holds it**,
and the manifest says so in `identity_limitation`.

An unreadable Delta identity for any table is fatal to the export: an inventory
that cannot say what it counted must not be given a name suggesting it can.

### Build commands

Prod (CSIRO HPC, ~461M Observation rows — the walltime rationale is in the job
script's header):

```
sbatch occurrences/count_occurrences.slurm
```

Demo (local, no queue; the demo warehouse has the same per-resource
`<ResourceType>.parquet` layout):

```
uv sync --extra hpc
uv run python3 -u occurrences/count_occurrences.py \
  --data /Users/nau025/warehouses/mimic-iv-demo/delta \
  --out-dir occurrences/demo \
  --inventory-out occurrences/demo/inventory \
  --release mimic-iv-demo-2.2 \
  --labels ig-resources
```

Then copy `<dataset>.csv` and `<dataset>.meta.json` into the orchestrator's
`src/services/resources/code_occurrences/` and set that environment's `dataset`
to the printed label. Do **not** set a `dataset` for an inventory that has not
been built and verified: the pool is then unavailable and code mapping stays
subtract-only, which is the correct behaviour, not a degraded one.

### Tests

`make test` — offline, no Spark, no warehouse. Every rule above is asserted over
hand-built synthetic counts, because the real extraction runs once against data
no laptop holds and is not a place to discover a contract bug.
