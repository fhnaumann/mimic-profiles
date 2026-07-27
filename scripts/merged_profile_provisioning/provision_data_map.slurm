#!/bin/bash
#SBATCH --job-name=provision-data-map
#SBATCH --time=2:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=64
#SBATCH --mem=503g
#SBATCH --exclusive

set -euo pipefail

# --- Fill these in --------------------------------------------------------
#   ACCOUNT (above): your project code, passed as `sbatch --account=<project>`.
#   REPO_DIR: the directory that holds pyproject.toml (where `uv run` resolves
#             the project env). Adjust to wherever the repo lives on the HPC cluster.
DATA="${SCRATCH:?}/mimic-on-fhir-delta/spark_warehouse"
# --------------------------------------------------------------------------

module load python/3.12.3
module load amazon-corretto/21.0.0.35.1

# Local-mode driver is the whole job — all work runs in this one JVM. These are
# the resources for the 64-core / 503 GB node. `pyspark-shell` suffix is required.
# The script's own os.environ.setdefault() defers to this exported value.
export PYSPARK_SUBMIT_ARGS="--master local[64] \
  --conf spark.driver.memory=400g \
  --conf spark.driver.maxResultSize=32g \
  --conf spark.sql.shuffle.partitions=256 \
  --conf spark.default.parallelism=256 \
  pyspark-shell"

cd $SCRATCH3DIR/mimic-on-fhir-delta

# data-map is internet-free (Delta read/write only). Run `verify` on the
# login node instead — it hits the terminology server, which the compute node
# cannot reach.
uv run python3 merged_profile_provisioning/provision.py \
  --mode data-map \
  --resource Observation \
  --data "$DATA"
