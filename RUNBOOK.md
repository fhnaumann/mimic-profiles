# RUNBOOK — CSIRO MIMIC-on-FHIR pipeline

This repo is a fork of [kindlab/mimic-profiles](https://github.com/kind-lab/mimic-profiles)
(the MIMIC-on-FHIR IG, upstream v1.3.0) carrying all CSIRO modifications, currently at
IG version **1.4.0-csiro**:

1. **Terminology** — the FHIR R4 CodeSystems for ICD-9-CM 2012 and ICD-10-CM
   2016–2019, the `mimic-diagnosis` ValueSet, and the ConceptMaps (with their
   enumerated target ValueSets) that map MIMIC's local codes to standard
   terminologies. These are **not built in this repository**: they come from a
   separate, non-public terminology mapping repository and are published to the
   terminology server from there.
2. **Required terminology bindings** on 12+ profile elements, derived from a coverage
   analysis of the **full** MIMIC-on-FHIR dataset (`scripts/binding-analysis/`).
3. **Condition.code migration** of the source data from the custom dotless MIMIC ICD
   CodeSystems to standard `icd-9-cm`/`icd-10-cm` with dotted codes, official displays,
   and pinned `Coding.version` (`scripts/icd-migration/`).

This document is the authoritative narrative and operating manual. It supersedes the
former `NOTES.md` and `MODIFICATION_PLAN.md`.

## 1. Pipeline DAG

Two environments, with a hard boundary:

- **Laptop** — internet, uv-managed Python, Java/SUSHI/Jekyll, reachability to the
  terminology server (`$ONTOSERVER_URL`, e.g. `https://ontoserver.example.org/fhir`).
- **HPC node** — holds the full MIMIC-on-FHIR Pathling Delta warehouse
  (`$MIMIC_WAREHOUSE`); Spark + Pathling available; **no internet, no terminology
  server**. Everything the node needs must be carried in as committed files.

```
[separate repo]  CodeSystems, ConceptMaps + ValueSets → $ONTOSERVER_URL ──┐
[laptop]  ig (sushi + _genonce.sh) ─→ package upload ──────────────────────┤
                                                                           ▼
[node]    read-time translation via ConceptMap/$translate
```

Condition/Procedure codes are **not** rewritten in the warehouse. Translation to
the standard ICD systems is a read-time concern (see §5); the node consumes the
published ConceptMaps rather than a migrated copy of the data.

Artifacts crossing the boundary:

| Artifact | Direction | Note |
|---|---|---|
| `scripts/binding-analysis/work-items.json` | laptop → node | drove the one-time phase-1 extraction |

The ConceptMaps and ValueSets the node translates with belong to the separate
terminology mapping repository (as does the node job that counts code
occurrences); they reach the node through the terminology server, not through
this repo.

Repeatable stages: `ig`. One-time analysis (documented in §5–6, re-runnable
manually but their conclusions are frozen into committed files): binding-analysis
phases 0–2.

## 2. Configuration

Copy `.env.example` to `.env`, adjust, and load with `set -a; source .env; set +a`.
CLI flags on the underlying scripts override the environment.

| Variable | Used by | Meaning |
|---|---|---|
| `ONTOSERVER_URL` | `_genonce.sh` (IG Publisher `-tx`), `scripts/merged_profile_provisioning/provision.py` | FHIR terminology server base URL |
| `PATHLING_URL` | `scripts/merged_profile_provisioning/provision.py` | Pathling server base URL |
| `MIMIC_WAREHOUSE` | `scripts/mimic-pipeline/` | Pathling Delta warehouse path (node) |

Make targets:

| Target | Where | What |
|---|---|---|
| `ig` | laptop | `sushi` + `./_genonce.sh` → `output/package.tgz` |

## 3. Toolchain

| Tool | Version used | Provenance |
|---|---|---|
| Python | 3.14 (`.python-version`), deps pinned in `uv.lock` (`pathling`, `striprtf`) | `uv sync` |
| SUSHI | 3.20.0 (FSH spec 3.0.0) | npm |
| IG Publisher | 2.2.10 | `_updatePublisher.sh` downloads the **latest** release into `input-cache/publisher.jar` — record the version on rebuilds (`java -jar input-cache/publisher.jar -v`) |
| Java | OpenJDK 21.0.11 (Zulu) | — |
| Jekyll | 4.4.1 | brew/gem (required by the publisher) |
| Ontoserver (server-side) | 6.27.3-SNAPSHOT, FHIR 4.0.1 | ontoserver.example.org |
| Spark/Pathling (node) | Pathling ≥ 7 (uses `view()`, not `extract()`) | node modules |

## 4. Stage details

### terminology (external)
The ICD CodeSystems, the ConceptMaps and their target ValueSets are built,
verified and published to `$ONTOSERVER_URL` from the separate, non-public
terminology mapping repository. This repository only consumes them: the IG
Publisher resolves bindings against the terminology server, and the node
translates through the published ConceptMaps.

### ig
`make ig`. Compiles `input/fsh/` with SUSHI, then `./_genonce.sh` runs the IG
Publisher with `-tx https://ontoserver.example.org/fhir` and a custom Java truststore
(§8). Only the publisher merges `input/resources/` (34 VS + 39 CS JSON) with the
FSH-compiled artifacts — all tooling must read `output/package.tgz`, never
`fsh-generated/` alone. Output: `output/package.tgz` (42+ VS, 39 CS, 27 SD at
1.4.0-csiro) and the QA report `output/qa.html`.
**Verify:** publisher QA report sane (no new errors vs. previous build);
`validator_cli` passes on the IG examples.

### IG package upload
The **IG package upload** (package.tgz → server) was performed by the terminology
server admin / manual FHIR upload, not by a script in this repo.
**Verify:** the checks recorded in `upload-verification-report.md` — resource counts
per type at the new version, `$expand` on `mimic-medication-with-unknown`,
`$validate-code` on `v3-NullFlavor#UNK`, `$lookup` spot-checks on admission-class/type.

## 5. What was changed and why (provenance)

### Version history
| Version | Meaning |
|---|---|
| 1.3.0 | upstream kindlab release (baseline) |
| 1.3.0-csiro.1 | fork bump so the unmodified rebuild is distinguishable on the terminology server (canonical URLs unchanged) |
| 1.4.0-csiro | bindings applied + Condition.code rebind; uploaded and verified 2026-07-13 (`upload-verification-report.md`) |

### Binding analysis (commit `433aca1`)
Evidence-driven: a field got a `required` binding only if the full MIMIC-on-FHIR data
was covered by the ValueSet ((system, code) membership, offline against
`package.tgz`). Pipeline: 131 candidate elements from package snapshots
(`work-items.json`) → Spark/Pathling distinct-code extraction on the node, partitioned
by `meta.profile` (38,102 distinct codes, `distinct-codes.ndjson`) → offline coverage
cross-check (`binding-report.{json,md}`). Of 131 candidates, 120 had no data; the 11
populated fields resolved as 4 bind / 1 repair→bind / 4 external-terminology /
2 no-binding. Decisions (full detail in `scripts/binding-analysis/FINDINGS.md`):

- **D1**: 8 required bindings applied; micro-test binds `valueCodeableConcept` only
  (not `value[x]`) so free-text results stay valid, using the standard `v3-NullFlavor` VS.
- **D2**: admin-hosp's single gap (`v3-NullFlavor#UNK`, 931 of 27.7M rows — intentional
  upstream ETL null-flavor) fixed via new wrapper VS `mimic-medication-with-unknown`
  (includes `mimic-medication` + `UNK`), bound on that profile only; the shared VS
  stays strict so UNK elsewhere signals an ETL regression.
- **D3**: medication-statement-ed gets **per-slice** bindings (GSN, ETC) with closed
  slicing instead of a merged VS; the NDC slice stays unbound (no server hosts the NDC
  CodeSystem; the slice still pins the system URI). ~68% of medrecon rows are
  text-only — a required CodeableConcept binding would have invalidated them.
- **D4**: vital-signs `component.code` bound to a new two-code VS
  `mimic-observation-component-vital` (BP LOINCs 8480-6/8462-4) rather than polluting
  the shared `mimic-observation-type-vital`.

Consumer note: bindings differ per profile on the same element (e.g.
`Observation.value[x]`), so downstream field→VS resolution must key on
`meta.profile`, never resource type alone; `binding-report.json` is the
machine-readable record including the 120 no-data elements.

### Condition.code migration (commit `e1f48e6`)
Motivation: the data carried custom dotless CodeSystems
(`mimic-diagnosis-icd{9,10}`); validation and mapping need the standard systems.
`Coding.version` is **pinned in the data** to the first ICD release year in which the
code validates (probe order = the ValueSet compose order), because Ontoserver 6.27
`$validate-code` checks a version-less coding against only one arbitrary version of a
multi-version ValueSet. Pinning makes every downstream `$validate-code` a single
direct call. Displays come from the pinned version.

Run on the full dataset (2026-07-16, `scripts/icd-migration/migration-report.json`):
5,655,376 Condition rows; 18,190 + 9,379 distinct source codes. Codings after:
2,445,489 on `icd-10-cm` (2016: 2,392,914, 2017: 37,818, 2018: 11,392, 2019: 2,864,
2024: 501) and 3,209,887 on `icd-9-cm` (all pinned 2012). Zero residual
custom-system codings; zero unmapped.

**False flags**: some codes labeled ICD-9 in MIMIC are actually ICD-10-CM (e.g.
`I50.9`, `J45.901`, `O86.12`, `R27.0`, `S01.312A`, `W01.190A`). `check_icd_codes.py`
detects these and `display-map.json` records them as moves; hence the ±5 asymmetry
between the icd9 before-count (3,209,892) and after-count, matched by +5 on icd10.
`display-map.json` is the committed decision record for every rewrite and relabel.

IG side: `Condition.code` re-bound from the custom `$MimicDiagnosisIcd` VS to the new
merged `mimic-diagnosis` ValueSet (includes both standard ICD systems, versions
pinned).

### Upload verification (2026-07-13)
All 27 SDs, 44 VS, 39 CS present at 1.4.0-csiro on velonto; `$expand`,
`$validate-code`, `$lookup` all functional; versions 1.3.0 / 1.3.0-csiro.1 /
1.4.0-csiro coexist on the server. Details: `upload-verification-report.md`.

## 6. One-time analysis scripts (frozen conclusions, manually re-runnable)

These produced the committed decision files. Re-running is possible but not part of
the reproduction path — the pipeline rebuilds from their committed outputs.

| Script | Runs on | Produces | Re-run |
|---|---|---|---|
| `scripts/binding-analysis/phase0_candidates.py` | laptop | `work-items.json` (131 candidates) | needs a built `output/package.tgz` |
| `scripts/binding-analysis/phase1_extract_distinct.py` | node | `distinct-codes.ndjson`, `extract-summary.json` | `uv run … --data $MIMIC_WAREHOUSE` (see `--help`); PathlingContext must own the SparkSession; Pathling 7+ `view()` not `extract()` |
| `scripts/binding-analysis/phase2_crosscheck.py` | laptop | `binding-report.{json,md}` | offline, stdlib-only over package.tgz + distinct-codes |
| `scripts/icd-migration/check_icd_codes.py` | laptop | `display-map.json`, `validation-*.csv`, `unmapped-*.csv` | `make check-codes` — requires the terminology deployed first (published from the separate terminology repository, plus the IG package upload); `--insecure` available for cert issues. `display-map.json` is already committed; regenerate only after a terminology change, and confirm `unmapped-*.csv` stay empty |

Current findings state: FINDINGS.md — all decisions D1–D4 resolved 2026-07-10 and
applied; binding-report.md — final coverage evidence; `unmapped-icd{9,10}.csv` — empty
(all codes resolve).

## 7. Releases

One git tag per IG version (`v1.4.0-csiro`, …) on the commit that built it. The GitHub
release attaches what git doesn't carry:

- `output/package.tgz` (the built IG package)
- `SHA256SUMS` over the assets

The ICD CodeSystem assets attached to earlier releases are built by the separate
terminology mapping repository.

Reproduce: checkout the tag → `make ig` → diff outputs against the assets
(package.tgz embeds build timestamps — compare its contents, not the archive bytes).

## 8. Known caveats

- **Truststore for the IG publisher**: velonto's TLS cert chains to the CSIRO internal
  CA, which Java doesn't trust by default. `_genonce.sh` passes
  `-Djavax.net.ssl.trustStore=input-cache/velonto-truststore.jks` (password
  `changeit`). The store is the JDK `cacerts` plus the velonto chain; it is not
  committed (lives in gitignored `input-cache/`). Rebuild it with:
  `cp $JAVA_HOME/lib/security/cacerts input-cache/velonto-truststore.jks &&
  openssl s_client -connect ontoserver.example.org:443 -showcerts </dev/null |
  awk '/BEGIN CERT/,/END CERT/' > /tmp/velonto-chain.pem &&
  keytool -importcert -noprompt -alias velonto -file /tmp/velonto-chain.pem
  -keystore input-cache/velonto-truststore.jks -storepass changeit`
- **`ONTOSERVER_URL` unset**: `_genonce.sh` falls back to
  `https://ontoserver.example.org/fhir`, and builds offline (`-tx n/a`) when that
  is unreachable; `check_icd_codes.py` refuses to run.
- **The HPC node has no internet**: node stages consume only committed files; don't
  add server calls to node-side scripts.
- **ICD-10-CM 2024** appears in the version pins (501 codings) but is **not built by
  the terminology build** — the 2024 CodeSystem was already present on velonto. A from-scratch
  server rebuild must load ICD-10-CM 2024 from elsewhere or those codings won't
  validate.
- **`_updatePublisher.sh` pulls the latest publisher** (no version pin) — record the
  version it fetched (currently 2.2.10) when rebuilding the IG.
- **`Requirements-fromNarrative.json`** (repo root, untracked) is referenced nowhere
  and its origin is unclear — left for Felix to keep or delete.
- **IG package upload is not scripted** — it was done server-side by the admin.
