#!/bin/bash
#SBATCH --job-name=count-unit-occurrences
#SBATCH --account=OD-221174
#SBATCH --time=0:30:00
#SBATCH --ntasks-per-node=1
#SBATCH --mem=256g
#SBATCH --cpus-per-task=32
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=Felix.Naumann@csiro.au
#SBATCH --output=/scratch3/nau025/mimic-on-fhir-delta/occurrences/unit-output/slurm-%j.out

set -euo pipefail
DATA="/scratch3/nau025/mimic-on-fhir-delta/spark_warehouse"
OUT="/scratch3/nau025/mimic-on-fhir-delta/occurrences/unit-output"
mkdir -p "$OUT"

module load python/3.12.3
module load amazon-corretto/21.0.0.35.1

export PYSPARK_SUBMIT_ARGS="--master local[32] \
  --conf spark.driver.memory=200g \
  --conf spark.driver.maxResultSize=16g \
  --conf spark.sql.shuffle.partitions=128 \
  --conf spark.default.parallelism=128 \
  pyspark-shell"

cd "$SCRATCH3DIR/mimic-on-fhir-delta"
uv run python3 -u occurrences/count_unit_occurrences.py \
  --data "$DATA" \
  --out-dir "$OUT"
