#!/bin/bash
#SBATCH --job-name=count-occurrences
#SBATCH --account=OD-221174
#SBATCH --time=0:30:00
#SBATCH --ntasks-per-node=1
#SBATCH --mem=503g
#SBATCH --cpus-per-task=64
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=Felix.Naumann@csiro.au
#SBATCH --output=/scratch3/nau025/mimic-on-fhir-delta/occurrences/slurm-%j.out

set -euo pipefail

# Ten column-pruned scans with a small group-by each, plus three choice-branch
# shape views and ONE JOIN (MedicationRequest -> Medication, following
# medicationReference). Measured: seven scans in 1m06s (2026-08-06), all ten plus
# the shapes and the join in 1m22s (2026-08-10). READ-ONLY — this job never
# writes to the warehouse.
#
# Those timings measured a warehouse that no longer exists: it was rebuilt on
# 2026-08-24, with every table's _delta_log rewritten to a single version 0. The
# export also added work they never covered — a per-resource shape scan for EVERY
# element (not just the three choice ones), and a distinct-patient count per
# stored identity, which is a shuffle-heavy count_distinct rather than a count.
# Both run twice over Observation's 461M rows, once for `code` and once for the
# exploded `component.code`.
#
# MEASURED on the current snapshot: 3m29s wall, peak RSS 129 GiB (job 32057655 on
# c275, 2026-09-09, all ten elements plus shapes, subjects and the join). The 30m
# request is that measurement with ~8.6x headroom; drop to 20m if queue priority
# matters. Row counts of the snapshot it was measured against: Observation
# 461,098,908, MedicationAdministration 36,733,071, MedicationRequest 15,416,901,
# Specimen 14,963,904, MedicationDispense 14,275,819, Condition 5,655,376,
# Procedure 3,354,975, MedicationStatement 2,598,365, Patient 299,712,
# Medication 26,027. Re-measure rather than re-assume if those move.
#
# Keep 64 CPUs / 503 GB: the memory is what the count_distinct shuffles need.
#
# HARD CAP: --time NEVER exceeds 2:00:00. If this job ever needs more than that,
# do not raise the walltime — add resources, up to --exclusive on a whole node,
# or stop and report it. A job that cannot fit in two hours with a full node is a
# job whose shape needs rethinking, not a longer reservation.
#
# ONE TASK, 64 CPUS — not 64 tasks. All the work runs in a single local-mode JVM
# (see PYSPARK_SUBMIT_ARGS below), so the cores belong to one task; asking for 64
# tasks describes an MPI job this is not, and combined with --exclusive it left
# the job PENDING on Resources rather than scheduling.
DATA="/scratch3/nau025/mimic-on-fhir-delta/spark_warehouse"

module load python/3.12.3
module load amazon-corretto/21.0.0.35.1

# Local-mode driver is the whole job — all work runs in this one JVM. These are
# the resources for the 64-core / 503 GB node. `pyspark-shell` suffix is required.
export PYSPARK_SUBMIT_ARGS="--master local[64] \
  --conf spark.driver.memory=400g \
  --conf spark.driver.maxResultSize=32g \
  --conf spark.sql.shuffle.partitions=256 \
  --conf spark.default.parallelism=256 \
  pyspark-shell"

# The REMOTE project's uv env (Python 3.12), never this repo's pyproject — and
# no `uv sync` here: compute nodes have no internet.
cd $SCRATCH3DIR/mimic-on-fhir-delta

# --release is the DATA RELEASE, not the snapshot identity. The script appends a
# digest of the observed Delta versions to it and prints the resulting dataset
# label; that label is the inventory's filename and the value the consuming
# repo's pathling_config.yaml `dataset` field must be set to. A release label
# alone would have named both the 2026-08-06 warehouse and the different one
# that replaced it, which is precisely why it is not the identity.
#
# --labels reads DICTIONARY LABELS ONLY, to fill in codes the warehouse stores
# with no display. rsync the repo's occurrences/ and ig-resources/ to the remote
# project first; if ig-resources is absent the export still runs and those codes
# stay searchable by code with an empty display, which is the intended
# degradation, not a failure.
LABEL_ARGS=()
if [ -d occurrences/ig-resources ]; then
  LABEL_ARGS=(--labels occurrences/ig-resources)
else
  echo "NOTE: occurrences/ig-resources absent — codes the warehouse stores with" \
       "no display will export with an empty display." >&2
fi

uv run python3 -u occurrences/count_occurrences.py \
  --data "$DATA" \
  --out-dir occurrences \
  --inventory-out occurrences/inventory \
  --release mimic-iv-3.1 \
  ${LABEL_ARGS[@]+"${LABEL_ARGS[@]}"}

