# Third-party notices

This file lists third-party material that is included in this repository, where it is, who
holds the rights to it, and the terms it is used under.

This repository is a fork. Its upstream content and the owner's additions are under different terms; both are described below.

Packages installed by a package manager (PyPI, npm) are not vendored here and are not listed;
their licences ship with the packages.

## Upstream: kind-lab/mimic-profiles and aehrc/mimic-profiles

This repository is a fork of [aehrc/mimic-profiles](https://github.com/aehrc/mimic-profiles)
(CSIRO's fork, with commits by John Grimes and Piotr Szul), itself a fork of
[kind-lab/mimic-profiles](https://github.com/kind-lab/mimic-profiles), the MIMIC-IV on FHIR
implementation guide by KinD Lab (Alex Bennett, Alistair Johnson, Philip van Damme and
contributors).

**The upstream repositories carry no licence file.** `sushi-config.yaml` (written upstream in
2022) declares `license: MIT` as ImplementationGuide metadata, but no licence text accompanies it.
The upstream content (the FSH profiles, `input/resources/` CodeSystems and ValueSets,
`input/pagecontent/`, `input/includes/`, `input/images/`, `template-kindlab/`, and the IG build
scripts) is used here under the terms of its upstream authors, whatever those are; this fork
grants no rights in it.

**The fork owner's additions and changes:** `scripts/terminology-mapping/`,
`scripts/binding-analysis/`, `scripts/mimic-pipeline/`, `scripts/merged_profile_provisioning/`, `agent_plans/`,
`.claude/skills/mapping-stream/`, `Makefile`, `pyproject.toml`, `uv.lock`, the top-level notes
(`NOTES.md`, `RUNBOOK.md`, `MODIFICATION_PLAN.md`, `TERMINOLOGY_STREAMS_DRAFT.md`,
`CODE-SEARCH-BUG-REPORT.md`, `upload-verification-report.md`, `Requirements-fromNarrative.json`,
`hard_c*.jsonl`), and edits to FSH profiles and CodeSystems/ValueSets (terminology bindings, ICD
source data; see the README's *CSIRO fork changes* section and `git log`).

## Template, logos and images

`template-kindlab/` is KinD Lab's IG template (its `package.json` declares `CC0-1.0`). It
includes `fhir-logo-www.png` (the FHIR logo) and `HL7_Canada.png`, trademarks of Health Level
Seven International and HL7 Canada, and the KinD Lab logos. The diagrams in `input/images/` are
upstream KinD Lab material.

## Clinical terminologies

The terminology mapping tables and their outputs (`scripts/terminology-mapping/conceptmaps/`,
`output/`, `eval/`) map MIMIC's local codes to standard terminologies and carry **codes with
their display names**: about 4,500 SNOMED CT concepts, about 6,000 LOINC codes, about 36,000
RxNorm concepts and about 600 UCUM units. `conceptmaps/medication-rxnorm-term-index.tsv/.json`
additionally indexes about 121,000 RxNorm names of about 73,000 concepts (RxNorm release
20231106, term types IN, PIN, MIN, BN, SCD, GPCK, BPCK), taken from a terminology server's RxNorm
code system.

`input/resources/` (upstream, partly edited) holds CodeSystems built from MIMIC-IV's dictionary
tables, including ICD-9-CM/ICD-10-CM/ICD-10-PCS codes with their descriptions, NDC and GSN drug
codes, and `CodeSystem-mimic-hcpcs-cd.json`: about 2,170 HCPCS codes, of which about 2,140 have the
form of CPT® codes. Those carry only a section-level label (for example "Digestive system"), not
the AMA CPT descriptors. CPT® is a registered trademark of the American Medical Association, which
holds the copyright in CPT.

### SNOMED CT

> This material includes SNOMED Clinical Terms® (SNOMED CT®) which is used by permission of
> SNOMED International. All rights reserved. SNOMED CT® was originally created by The College of
> American Pathologists. "SNOMED" and "SNOMED CT" are registered trademarks of SNOMED
> International.

Use of SNOMED CT content requires the user to hold an appropriate licence: a SNOMED International
Affiliate Licence, or a national release licence in a SNOMED International Member country. See
<https://www.snomed.org/get-snomed>.

### LOINC

> This material contains content from LOINC® (http://loinc.org). LOINC is copyright © Regenstrief
> Institute, Inc. and the Logical Observation Identifiers Names and Codes (LOINC) Committee and is
> available at no cost under the license at http://loinc.org/license. LOINC® is a registered
> United States trademark of Regenstrief Institute, Inc.

### RxNorm

> This product uses publicly available data from the U.S. National Library of Medicine (NLM),
> National Institutes of Health, Department of Health and Human Services; NLM is not responsible
> for the product and does not endorse or recommend this or any other product.

### UCUM

> This product includes all or a portion of the UCUM table, UCUM codes, and UCUM definitions or is
> derived from it, subject to a license from Regenstrief Institute, Inc. and The UCUM
> Organization. Your use of the UCUM table, UCUM codes, UCUM definitions also is subject to this
> license, a copy of which is available at http://unitsofmeasure.org. The current complete UCUM
> table, UCUM Specification are available for download at http://unitsofmeasure.org. The UCUM
> table and UCUM codes are copyright © 1995-2013, Regenstrief Institute, Inc. and the Unified
> Codes for Units of Measures (UCUM) Organization. All rights reserved.

### ICD-9-CM, ICD-10-CM, ICD-10-PCS

ICD-9-CM and ICD-10-CM (published by the CDC National Center for Health Statistics) and
ICD-10-PCS (published by the Centers for Medicare & Medicaid Services) are works of the U.S.
government and are in the public domain in the United States. No notice is required and no
restriction applies. ICD-10-CM is the U.S. clinical modification of the WHO's ICD-10, used by NCHS
with WHO's permission.

### National Drug Code (NDC)

NDC codes are published by the U.S. Food and Drug Administration (National Drug Code Directory), a
work of the U.S. government in the public domain. No notice is required.

### HL7® FHIR® and HL7 Terminology (THO)

FHIR resource and element names, and codes from FHIR and HL7 Terminology (`http://hl7.org/fhir/...`,
`http://terminology.hl7.org/...`) code systems, come from the FHIR specification and HL7
Terminology, which HL7 publishes under the Creative Commons CC0 public-domain dedication
(<https://www.hl7.org/fhir/license.html>). No notice is required. HL7® and FHIR® are registered
trademarks of Health Level Seven International; their use does not constitute endorsement by HL7.

## MIMIC-IV data

### MIMIC-on-FHIR CodeSystems

The `http://mimic.mit.edu/fhir/mimic/CodeSystem/*` CodeSystems (upstream) enumerate MIMIC-IV's
local codes and labels: item ids and labels of `d_items`/`d_labitems`, medication names, drug
codes, microbiology codes and similar. They are dictionary content of MIMIC-IV, published as part
of the implementation guide. The same dictionaries ship with the open demo releases.

### First Databank GSN and ETC codes

The `mimic-medication-gsn` and `mimic-medication-etc` MIMIC-on-FHIR code systems carry First
Databank Generic Sequence Numbers (GSN) and Enhanced Therapeutic Classification (ETC) codes and
class names, as they occur in MIMIC-IV and MIMIC-IV-ED. GSN and ETC are identifiers of First
Databank, Inc., which holds the rights in them; they appear here only as they appear in those
datasets.

### MIMIC-IV demo datasets (Open Database License)

The open MIMIC-IV demo releases on PhysioNet are licensed under the **Open Data Commons Open
Database License v1.0 (ODbL)**, <https://opendatacommons.org/licenses/odbl/1-0/>:

- **MIMIC-IV Clinical Database Demo**, version 2.2. Johnson A, Bulgarelli L, Pollard T, Horng S,
  Celi LA, Mark R. PhysioNet (2023). <https://doi.org/10.13026/dp1f-ex47>
- **MIMIC-IV-ED Demo**, version 2.2. Johnson A, Bulgarelli L, Pollard T, Celi LA, Horng S, Mark R.
  PhysioNet (2023). <https://doi.org/10.13026/jzz5-vs76>
- **MIMIC-IV Clinical Database Demo on FHIR**. Bennett A, Ulrich H, Wiedekopf J, Szul P, Grimes J,
  Johnson A. PhysioNet. <https://physionet.org/content/mimic-iv-fhir-demo/>

PhysioNet: Goldberger A, Amaral L, Glass L, Hausdorff J, Ivanov PC, Mark R, Mietus JE, Moody GB,
Peng CK, Stanley HE. PhysioBank, PhysioToolkit, and PhysioNet: Components of a new research
resource for complex physiologic signals. *Circulation* 101(23):e215–e220 (2000). PhysioNet's
current standard citation is Pollard T, Moody BE, Lehman L, et al. PhysioNet as a global platform
for biomedical research. *Nature Health* (2026). <https://doi.org/10.1038/s44360-026-00096-z>

> Contains information from the MIMIC-IV Clinical Database Demo, which is made available here
> under the Open Database License (ODbL).

The demo-derived files listed below are derived databases of these releases. Under ODbL §4.4 a
derived database that is publicly used must itself be offered under the ODbL, so these data files
are subject to the ODbL v1.0. This applies to those data files only, not to the code in this
repository.

Demo-derived files in this repository:

- `scripts/terminology-mapping/occurrences/demo/**` (`code-occurrences.csv`,
  `reference-occurrences.csv`, `element-shapes.csv`, `occurrence-summary.json`, and
  `inventory/mimic-iv-demo-2.2-1e912c8b29d3.{csv,meta.json}`): aggregate code-occurrence counts of
  a MIMIC-on-FHIR warehouse built from the demo release.

### MIMIC-IV (credentialed)

MIMIC-IV (Johnson A, Bulgarelli L, Pollard T, Horng S, Celi LA, Mark R. MIMIC-IV, version 2.2.
PhysioNet (2023), <https://doi.org/10.13026/6mm1-ek67>; Johnson AEW, Bulgarelli L, Shen L, et al.
MIMIC-IV, a freely accessible electronic health record dataset. *Sci Data* 10, 1 (2023)) is
available only to credentialed users under the **PhysioNet Credentialed Health Data License 1.5.0**
and the PhysioNet Credentialed Health Data Use Agreement 1.5.0. No patient-level data from it is
included in this repository.

Full-dataset content appears here only in aggregate form: occurrence and value-shape
summaries (`occurrences/occurrence-summary.json`, `occurrences/prod/`, `unit-output/`,
`valueshapes/value-shape-summary.json`), the data-weighted `output/mapping-statistics.csv` and
`output/occurrence-buckets.csv`, and the binding analysis (`scripts/binding-analysis/`). The
full-data inventories themselves are kept out of the repository (`.gitignore`).
